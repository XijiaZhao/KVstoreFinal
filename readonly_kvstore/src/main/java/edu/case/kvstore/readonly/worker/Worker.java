package edu.case.kvstore.readonly.worker;

import edu.case.kvstore.readonly.rpc.*;
import edu.case.kvstore.readonly.util.Record;
import edu.case.kvstore.readonly.util.UUIDGenerator;
import edu.case.kvstore.readonly.worker.bucketholder.HDFSBucketHolder;
import edu.case.kvstore.readonly.worker.bucketstorage.BucketStorage;
import edu.case.kvstore.readonly.worker.bucketstorage.SimpleBucketStorage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Worker {
    private volatile static Worker workerSingleton;
    private RPCInterface.MasterReply.MasterState masterState;

    private volatile WorkerState state;
    private int workerID = -1; //当前worker被分配到的ID
    private String workerUUID = "";
    private String hostName = "";
    private MasterClient masterClient;

    private WorkerInfo[] workerInfoList;
    private Properties conf; //保存配置信息
    private TimerThread timer;

    private QueryService queryService;   //在startNewService中初始化
    private QueryClient queryClient;    //在startNewService中初始化
    private BucketStorage bucketStorage; //在startNewService中初始化
    private int queryServicePort = -1;   //在startNewService中初始化


    private Logger logger;

    /* conf的配置
    master.hostname (master的hostname)   master.port (MasterService的port)
    num.workers (worker的总数量 N)     num.buckets (buckets的总数B)  num.backups(备份的个数）
    bucketstorage.classname (bucketstorage类的类名)    bucketholder.classname (bucketholder类的类名)
    bucketholder.hdfsDataFold (文件存储在hdfs上的目录)   queryclient.classname (用于指定QueryClient的类名)*/

    /*worker向下传递conf时添加了
    worker.UUID (worker的UUID，保证单机调试时不同Worker的storage存储位置不同）*/
    private Worker(Properties conf, boolean active) throws IOException {
        this.conf = conf;
        this.logger = LoggerFactory.getLogger(Worker.class.getName());
        //配置本地信息
        try {
            this.hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.error(e.toString());
            logger.error("fail to get local hostName");
            System.exit(1);
        }
        this.workerUUID = UUIDGenerator.generateUUID(hostName, active);
        this.conf.setProperty("worker.UUID", this.workerUUID);
        this.workerID = -1;
        this.workerInfoList = new WorkerInfo[Integer.parseInt(conf.getProperty("num.workers"))];
        for (int i = 0; i < workerInfoList.length; i++) {
            workerInfoList[i] = new WorkerInfo();
        }
        this.state = WorkerState.IDLE; //state保存当前worker的状态，初始化为IDLE

        try {
            Class cla = Class.forName(conf.getProperty("queryclient.classname"));
            this.queryClient = (QueryClient) cla.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            logger.warn(e.toString());
            logger.warn("Worekr构造函数，创建queryClient出错");
            e.printStackTrace();
            throw new IOException(e);
        }
        this.queryClient.init(conf, this);

        //创建连接Master的MasterService的masterClient
        this.masterClient = new MasterClient(conf.getProperty("master.hostname"),
                Integer.parseInt(conf.getProperty("master.port")));
        heartBeat();
        //Timer线程，该线程每隔3s调用一次Worker的heartbeat方法
        this.timer = new TimerThread();
        this.timer.setDaemon(true);
        this.timer.start();

        /*Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("shutting down Worker since JVM is shutting down");
            Worker.this.shutdown();
            System.err.println("worker shut down");
            logger.info("worker shut down ");
        }));*/
    }

    public static Worker getSingleton(Properties conf, boolean active) throws IOException {
        if (workerSingleton == null) {
            synchronized (Worker.class) {
                if (workerSingleton == null) {
                    workerSingleton = new Worker(conf, active);
                }
            }
        }
        return workerSingleton;
    }

    public void waitSystemInited() throws InterruptedException {
        while (masterState != RPCInterface.MasterReply.MasterState.WORKING ||
                !checkWorkerPortAvailable()) {
            Thread.sleep(1000);
        }
    }

    private boolean checkWorkerPortAvailable() {
        for (WorkerInfo info : workerInfoList) {
            if (info.getQueryServicePort() == -1) {
                return false;
            }
        }
        return true;
    }

    public int getWorkerID() {
        return workerID;
    }

    public BucketStorage getBucketStorage() {
        return bucketStorage;
    }

    public WorkerState getState() {
        return state;
    }

    public int getSLEEP_TIME_IN_MILLISECOND() {
        return timer.getSLEEP_TIME_IN_MILLISECOND();
    }

    public void shutdown() {
        timer.exit = true;
        try {
            timer.join();
            closeExistingService(); //QueryService
            masterClient.shutdown(); //MasterClient
            if (queryClient != null) {
                queryClient.close();
            }
            queryClient = null;
        } catch (InterruptedException e) {
            logger.warn("Exception from {}", Thread.currentThread().getName());
            e.printStackTrace();
        }
        workerInfoList = null;
    }

    public static void closeSingleton() {
        if (workerSingleton != null) {
            synchronized (Worker.class) {
                workerSingleton.shutdown();
                workerSingleton = null;
            }
        }
    }

    public byte[] query(byte[] key) throws IOException {
        byte[] val = null;
        val = queryClient.query(key);
        return val;
    }

    private void heartBeat() {
        //Worker的QueryService服务没有初始化完成时，不对外提供port
        int externalServicePort = (getState() == WorkerState.WORKING) ? queryServicePort : -1;
        RPCInterface.WorkerInfoState localState = RPCInterface.WorkerInfoState.newBuilder()
                .setMyWorkerID(workerID)
                .setHostName(hostName)
                .setQueryServicePort(externalServicePort)
                .setWorkerUUID(workerUUID)
                .build();
        //调用Master的接口
        RPCInterface.MasterReply masterReply = masterClient.reportHeartBeat(localState);

        if (masterReply == null) {
            logger.error("heartBeatEnding, masterReply为空, UUID: {}", workerUUID);
            return;
        }

        if (workerID != masterReply.getWorkerID() || masterState != masterReply.getMasterState()) {
            logger.info("Message form Master: AllocatedID: {}, MasterServiceState: {}, WorkerUUID: {}",
                    masterReply.getWorkerID(), masterReply.getMasterState().toString(), workerUUID);
        }

        //暂存原有状态
        WorkerState oldState = state;
        RPCInterface.MasterReply.MasterState oldMasterState = masterState;
        int oldWorkerID = workerID;

        //更新本地状态
        masterState = masterReply.getMasterState();
        List<RPCInterface.WorkerInfoState> workerLatestStatus = masterReply.getWorkersList();
        for (int i = 0; i < workerLatestStatus.size(); i++) {
            workerInfoList[i].setFromWorkerInfoState(workerLatestStatus.get(i));
        }

        int replyID = masterReply.getWorkerID();
        if (state == WorkerState.IDLE && replyID >= 0) {
            state = WorkerState.INIT;
            workerID = replyID;
            Thread startNewServiceThread = new Thread(this::startNewService);
            startNewServiceThread.start();
        } else if (state == WorkerState.ERROR) {
            state = WorkerState.IDLE;
            workerID = -1;
        } else if (state == WorkerState.WORKING && replyID == -1) {
            closeExistingService();
            state = WorkerState.IDLE;
            workerID = -1;
        } else if (state == WorkerState.WORKING && replyID >= 0) {
            if (replyID != workerID) {
                closeExistingService();
                workerID = replyID;
                state = WorkerState.INIT;
                Thread startNewServiceThread = new Thread(this::startNewService);
                startNewServiceThread.start();
            }
        }

        queryClient.updateWorkersInfo(workerInfoList);
        if (state != oldState || workerID != oldWorkerID || masterState != oldMasterState) {
            logger.info("heartBeatEnding, new {}", this.toString());
        }
    }

    private void startNewService() {
        logger.info("startQueryService begin, workerUUID: {}\n", workerUUID);
        try {
            //构建BucketStorage，反射机制，根据bucketstorage.classname的类名
            Class cla = Class.forName(conf.getProperty("bucketstorage.classname"));
            bucketStorage = (BucketStorage) cla.newInstance();
            //调用bucketStorage的init方法，初始化bucketStorage
            bucketStorage.init(conf, workerID);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            //反射执行过程出错
            logger.warn("startNewService, {} : {}", e.getClass().getName(), e.getLocalizedMessage());
            logger.warn("startNewService, 使用反射build bucketStorage出错! startQueryService end");
            bucketStorage = null;
            queryService = null;
            state = WorkerState.ERROR;
            return;
        } catch (IOException ex) {
            //调用init异常
            //如果该方法抛出异常，在日志中打印WARNING信息，bucketStorage和queryService = NULL，state=ERROR，退出。
            logger.warn("WorkerUUID: {} BucketStorage init Exception", workerUUID);
            bucketStorage = null;
            queryService = null;
            state = WorkerState.ERROR;
            return;
        }
        //创建QueryService对象
        //QueryService的端口号从10000-50000之前取一个随机数
        queryServicePort = (new Random()).nextInt(40001) + 10000;
        try {
            this.queryService = new QueryService(queryServicePort, bucketStorage);
        } catch (IOException e) {
            logger.warn("queryService 启动失败, WorkerID: {}", workerID);
            //e.printStackTrace();
            // 如果该方法抛出异常，则调用bucketStorage的close方法
            try {
                bucketStorage.close();
            } catch (IOException ex) {
                //ex.printStackTrace();
                logger.warn("close bucketStorage failed");
            }
            // bucketStorage和queryService = NULL，state=ERROR，退出。
            bucketStorage = null;
            queryService = null;
            state = WorkerState.ERROR;
            shutdown();
            return;
        }
        //BucketStorage,QueryService均成功......
        state = WorkerState.WORKING;
        logger.info("New QueryService started, {}\n", toString());
    }

    private void closeExistingService() {
        try {
            //如果bucketStorage != NULL，调用bucketStorage的close方法，设置bucketStorage = NULL
            if (bucketStorage != null) {
                bucketStorage.close();
                bucketStorage = null;
            }
            //如果queryService != NULL，调用queryService对象的close方法，设置queryService = NULL
            if (queryService != null) {
                queryService.stop();
                queryService = null;
            }
        } catch (IOException | InterruptedException e) {
            logger.warn("QueryService关闭时出错, WorkerInfo: {}\n", this.toString());
        } finally {
            //在执行的过程中捕获所有异常，先转变状态为IDLE，再退出
            state = WorkerState.IDLE;
        }
    }

    @Override
    public String toString() {
        return "WorkerInfo:[WorkerState: " + state.toString() +
                ", masterState: " + masterState.toString() +
                ", WorkerID: " + workerID +
                //", WorkerUUID: " + workerUUID +
                ", QueryServicePort: " + queryServicePort + "]";
    }


    //负责与Master的MasterService通信
    // 这个类是Worker与Master通信功能的抽象，gRPC中并没有必要创建单独的Client类，这里构造一个，把功能单独拿出来
    class MasterClient {
        private final ManagedChannel channel;
        private final MasterServiceGrpc.MasterServiceBlockingStub blockingStub;

        private Logger logger;

        public MasterClient() {
            this("127.0.0.1", 50051);
        }

        public MasterClient(String host, int port) {
            this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
        }

        public MasterClient(ManagedChannelBuilder<?> channelBuilder) {
            channel = channelBuilder.build();
            blockingStub = MasterServiceGrpc.newBlockingStub(channel);
            logger = LoggerFactory.getLogger(MasterClient.class.getName());
        }

        public void shutdown() throws InterruptedException {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }

        //gRPC过程调用
        public RPCInterface.MasterReply reportHeartBeat(RPCInterface.WorkerInfoState workerInfoState) {
            RPCInterface.MasterReply masterReply;
            try {
                masterReply = blockingStub.reportHeartbeat(workerInfoState);
            } catch (StatusRuntimeException e) {
                logger.warn("RPC failed: {}", e.getStatus());
                return null;
            }
            return masterReply;
        }
    }

    class TimerThread extends Thread {
        private final int SLEEP_TIME_IN_MILLISECOND = 3000;
        public volatile boolean exit = false;

        @Override
        public void run() {
            while (!exit) {
                //logger.info("WorkerUUID: {} HeartBeating～～～～", workerUUID);
                long t0 = System.currentTimeMillis();
                heartBeat();
                long t1 = System.currentTimeMillis();
                //logger.info("workerUUID: {} HeartBeatEnd-------\n", workerUUID);
                try {
                    if ((t1 - t0) < SLEEP_TIME_IN_MILLISECOND) {
                        Thread.sleep(SLEEP_TIME_IN_MILLISECOND - (t1 - t0));
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        public int getSLEEP_TIME_IN_MILLISECOND() {
            return SLEEP_TIME_IN_MILLISECOND;
        }
    }

    private void run() throws IOException, InterruptedException {
        Record[] records = new Record[100];
        BufferedReader bufferedReader = new BufferedReader(new FileReader("/home/hadoop/readonly_kvstore/KVID.txt"));
        String line = bufferedReader.readLine();
        int cntRecord = 0;
        while (line != null && cntRecord < 100) {
            String[] str = line.split(" ");
            byte[] personName = str[0].getBytes(StandardCharsets.UTF_8);
            byte[] personVal = str[1].getBytes(StandardCharsets.UTF_8);
            int bucketID = Integer.parseInt(str[2]);

            records[cntRecord] = new Record(personName, personVal, bucketID);
            cntRecord++;
            line = bufferedReader.readLine();
        }

        for (int i = 0; i < 1000; i++) {
            int index = (new Random()).nextInt(100);
            byte[] getVal = null;
            try {
                getVal = query(records[index].getKey());
                String res = (Arrays.equals(getVal, records[index].getValue())) ? "SUCCESS..." : "FAILED...";
                logger.info("Key: {}, Value: {}, BucketID: {}, Index: {}, Res: {}, ", new String(records[index].getKey()),
                        new String(records[index].getValue()), records[index].getBucket(), index, res);
            } catch (IOException e) {
                logger.warn(e.toString());
            }
            Thread.sleep(200);
        }
    }

    public static void testQuery_Random() throws IOException, InterruptedException {
        Record[] records = new Record[100];
        BufferedReader bufferedReader = new BufferedReader(new FileReader("KVID.txt"));
        String line = bufferedReader.readLine();
        int cntRecord = 0;
        while (line != null && cntRecord < 100) {
            String[] str = line.split(" ");
            byte[] personName = str[0].getBytes(StandardCharsets.UTF_8);
            byte[] personVal = str[1].getBytes(StandardCharsets.UTF_8);
            int bucketID = Integer.parseInt(str[2]);

            records[cntRecord] = new Record(personName, personVal, bucketID);
            cntRecord++;
            line = bufferedReader.readLine();
        }

        Properties conf = new Properties();
        conf.setProperty("master.hostname", "127.0.0.1");
        conf.setProperty("master.port", "50051");
        conf.setProperty("num.workers", "3");
        conf.setProperty("num.buckets", "12");
        conf.setProperty("num.backups", "2");
        conf.setProperty("bucketstorage.classname", SimpleBucketStorage.class.getName());
        conf.setProperty("bucketholder.classname", HDFSBucketHolder.class.getName());

        conf.setProperty("bucketholder.hdfsDataFold", "hdfs://localhost:9000/user/hadoop/buckets");
        conf.setProperty("queryclient.classname", QueryClient.class.getName());
        Worker aworker = Worker.getSingleton(conf, true);
        aworker.waitSystemInited();
        //aworker.queryClient.sayHello();

        while (true) {
            int index = (new Random()).nextInt(100);
            byte[] getVal = null;
            try {
                //getVal = aworker.query(records[list[index]].getKey());
                getVal = aworker.query(records[index].getKey());
                String res = (Arrays.equals(getVal, records[index].getValue())) ? "SUCCESS..." : "FAILED...";
                aworker.logger.info("Key: {}, Value: {}, BucketID: {}, Index: {}, Res: {}, ", new String(records[index].getKey()),
                        new String(records[index].getValue()), records[index].getBucket(), index, res);
                aworker.logger.info("expect: {}, get: {}\n", new String(getVal), new String(records[index].getValue()));
            } catch (IOException e) {
                aworker.logger.warn(e.toString());
            }
            index = (index + 1) % 3;
            Thread.sleep(200);
        }
    }

    public void testQuery() throws InterruptedException {
        Record[] recordList = new Record[3];
        recordList[0] = new Record("东方不败".getBytes(), String.valueOf(2.6788015).getBytes(), 0);
        recordList[1] = new Record("周颠".getBytes(), String.valueOf(3.0444589).getBytes(), 1);
        recordList[2] = new Record("心砚".getBytes(), String.valueOf(4.4911866).getBytes(), 2);
        int index = 0;
        while (true) {
            byte[] getVal = null;
            try {
                getVal = query(recordList[index].getKey());
                String res = (Arrays.equals(getVal, recordList[index].getValue())) ? "SUCCESS..." : "FAILED...";
                logger.info("Key: {}, Value: {}, BucketID: {}, Index: {}, Res: {}, ", new String(recordList[index].getKey()),
                        new String(recordList[index].getValue()), recordList[index].getBucket(), index, res);
                logger.info("expect: {}, get: {}\n", new String(getVal), new String(recordList[index].getValue()));
            } catch (IOException e) {
                logger.warn(e.toString());
            }
            index = (index + 1) % 3;
            Thread.sleep(200);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties conf = new Properties();
        conf.setProperty("master.hostname", "127.0.0.1");
        conf.setProperty("master.port", "50051");
        conf.setProperty("num.workers", "3");
        conf.setProperty("num.buckets", "12");
        conf.setProperty("num.backups", "2");
        conf.setProperty("bucketstorage.classname", SimpleBucketStorage.class.getName());
        conf.setProperty("bucketholder.classname", HDFSBucketHolder.class.getName());

        conf.setProperty("bucketholder.hdfsDataFold", "hdfs://localhost:9000/user/hadoop/buckets");
        conf.setProperty("queryclient.classname", QueryClient.class.getName());
        Worker aworker = Worker.getSingleton(conf, true);
        aworker.waitSystemInited();

        aworker.testQuery();
    }
}