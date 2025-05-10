package edu.case.kvstore.readonly.rpc;

import edu.case.kvstore.readonly.util.BucketPartitioner;
import edu.case.kvstore.readonly.worker.Worker;
import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class QueryClient {
    protected Properties conf; // 保存关键配置
    protected Worker worker; // 保存包含当前queryClient的worker的引用
    protected WorkerInfo[] workersInfo; // 用于保存从当前系统中的workers信息
    protected QueryServiceClient[] queryServiceClients; // 用于连接各个worker的QueryService的gRPC client
    protected ReadWriteLock InfosAndClientsRWLock;  // 读写锁，用于控制workerInfo、rpcClients的多线程并发访问；

    protected int numBuckets;
    protected int numWorkers;
    protected int numBackups;
    private Logger logger = LoggerFactory.getLogger(QueryClient.class);

    public void init(Properties conf, Worker worker) {
        this.conf = conf;
        this.worker = worker;
        this.InfosAndClientsRWLock = new ReentrantReadWriteLock();
        this.numBuckets = Integer.parseInt(conf.getProperty("num.buckets"));
        this.numWorkers = Integer.parseInt(conf.getProperty("num.workers"));
        this.numBackups = Integer.parseInt(conf.getProperty("num.backups"));

        this.workersInfo = new WorkerInfo[Integer.parseInt(conf.getProperty("num.workers"))];
        for (int i = 0; i < workersInfo.length; i++) {
            workersInfo[i] = new WorkerInfo(); //默认构造worker的workerID = -1，UUID = "";
        }
        queryServiceClients = new QueryServiceClient[Integer.parseInt(conf.getProperty("num.workers"))];
        //未初始化的QueryServiceClient被设置成null
    }

    // 更新所有的Worker信息，由Worker的heartbeat方法触发
    // 在每个心跳之后，Worker接收Master端返回的MasterReply信息，根据MasterReply信息更新QueryClient里的workerInfo数组。
    public void updateWorkersInfo(WorkerInfo[] workerInfoFromMaster) {
        //InfosAndClientsRWLock.readLock().lock();
        //logger.info("updateClientList...");
        //logger.info("old WorkersInfo: \n{}", workerListToString(workersInfo));
        //logger.info("workerInfoFromMaster...\n{}", workerListToString(workerInfoFromMaster));
        //logger.info("old queryServiceClients...{}", clientListToString());
        boolean isModified = false;
        if (workerInfoFromMaster.length != workersInfo.length) {
            logger.error("workerInfoFromMaster.length != workersInfo.length");
        }
        for (int i = 0; i < workerInfoFromMaster.length; i++) {
            if (isChanged(workersInfo[i], workerInfoFromMaster[i])) {
                isModified = true;
                break;
            }
        }
        //InfosAndClientsRWLock.readLock().unlock();
        if (!isModified) {
            //logger.info("没有检测到Worker状态改变，退出updateWorkersInfo()...");
            //logger.info("------------------------------------------------------------------------------------------------\n");
            return;
        }
        //logger.info("正在获取lock...");
        InfosAndClientsRWLock.writeLock().lock();
        //logger.info("获取到lock...");
        for (int i = 0; i < workersInfo.length; i++) {
            //如果该Worker信息改变
            if (isChanged(workersInfo[i], workerInfoFromMaster[i])) {
                //如果对应的QueryServiceClient的链接已经建立
                if (queryServiceClients[i] != null) {
                    //关闭它
                    try {
                        queryServiceClients[i].shutdown();
                        queryServiceClients[i] = null;
                    } catch (InterruptedException e) {
                        logger.warn(e.toString());
                        logger.warn("QueryServiceClient关闭时出错, {}", queryServiceClients[i].toString());
                    }
                }
                if (workerInfoFromMaster[i].getQueryServicePort() > 0) {
                    //根据新的worker的信息更新QueryServiceClient
                    String hostName = workerInfoFromMaster[i].getHostName();
                    int port = workerInfoFromMaster[i].getQueryServicePort();
                    queryServiceClients[i] = new QueryServiceClient(hostName, port);
                }
                //测试链接搭建成功与否
                //*****************
                //更新WorkerInfo信息
                workersInfo[i].copyFromWorkerInfo(workerInfoFromMaster[i]);
            }
        }
        //logger.info("new WorkersInfo: {}", workerListToString(workersInfo));
        //logger.info("new queryServiceClients...{}", clientListToString());
        //logger.info("------------------------------------------------------------------------------------------------\n");
        InfosAndClientsRWLock.writeLock().unlock();
        //logger.info("已释放lock...");
    }

    public String clientListToString() {
        StringBuilder strbder = new StringBuilder();
        for (QueryServiceClient client : queryServiceClients) {
            if (client != null) {
                strbder.append(client.toString()).append("\n");
            } else {
                strbder.append("NULL").append("\n");
            }
        }
        return strbder.toString();
    }

    public String workerListToString(WorkerInfo[] aworkerList) {
        StringBuilder strbder = new StringBuilder();
        for (WorkerInfo worker : aworkerList) {
            strbder.append(worker.toString()).append("\n");
        }
        return strbder.toString();
    }

    // 查询一个KV对
    public byte[] query(byte[] key) throws IOException {
        //HashFunction murmur3 = Hashing.murmur3_32();
        //int targetBucketID = Math.abs(murmur3.hashBytes(key).hashCode()) % numBuckets;
        int targetBucketID = BucketPartitioner.getBucketID(key, numBuckets);
        int targetWorkerID = targetBucketID % numWorkers;

        logger.info("开始尝试查询Key: [{}], targetWorkerID: {}", new String(key, StandardCharsets.UTF_8), targetWorkerID);
        Pair<byte[], Boolean> res = queryAvailableWorker(key, targetWorkerID);
        byte[] value = res.getKey();
        boolean initUncompleted = res.getValue();

        //没有查询到结果且有Worker没有初始化完成时进行循环
        int cnt = 0;
        while (value == null && initUncompleted && cnt < 3) {
            try {
                Thread.sleep(worker.getSLEEP_TIME_IN_MILLISECOND());
            } catch (InterruptedException e) {
                logger.info(e.toString());
                throw new IOException(e);
            }
            res = queryAvailableWorker(key, targetWorkerID);
            value = res.getKey();
            initUncompleted = res.getValue();
            cnt++;
        }

        //查询到了结果，返回
        if (value != null) {
            return value;
        }
        //未查询到结果，Worker均已经初始化完成
        else {
            //遍历完仍然查不到value
            logger.error("All workers responsible for key: {} are down", new String(key, StandardCharsets.UTF_8));
            throw new IOException("All workers responsible for key: " + new String(key, StandardCharsets.UTF_8) + " are down");
        }
    }

    private Pair<byte[], Boolean> queryAvailableWorker(byte[] key, int targetWorkerID) {
        byte[] value = null;
        boolean initUncompleted = false;
        InfosAndClientsRWLock.readLock().lock();
        for (int i = 0; i < numBackups; i++) {
            int queryWorkerID = (targetWorkerID + i) % numWorkers;
            try {
                //如果当前WorkerID保存了targetBucketID的备份，就从本地读取
                if (worker.getWorkerID() == queryWorkerID) {
                    if (worker.getState() != WorkerState.WORKING) {
                        initUncompleted = true;
                        logger.warn("本地QueryService服务初始化未完成！");
                    } else {
                        logger.info("将在本地查询 Key: [{}] WorkerID: {}", new String(key, StandardCharsets.UTF_8),
                                worker.getWorkerID());
                        value = worker.getBucketStorage().get(key);
                    }
                }
                //否则gRPC调用
                else {
                    if (workersInfo[queryWorkerID].getWorkerID() == -1 || queryServiceClients[queryWorkerID] == null) {
                        //对方服务完成未初始化
                        //queryServiceClient的更新在Worker的heartBeat末尾部分，updateWorekersInfo将QueryServicePort > 0的Worker生成Client
                        initUncompleted = true;
                        logger.warn("未存储WorkerID： {} 信息------>远程WorkerID: {} 未完成QueryService的初始化!",
                                queryWorkerID, queryWorkerID);
                    } else {
                        logger.info("将远程调用查询 Key: [{}], workerID: {}",
                                new String(key, StandardCharsets.UTF_8), queryWorkerID);
                        RPCInterface.QueryReply reply = queryServiceClients[queryWorkerID].get(key);
                        if (reply != null) {
                            switch (reply.getErrno()) {
                                case OK:
                                    value = reply.getVal().toByteArray();
                                    break;
                                case ERROR:
                                    logger.warn("Storage get过程出错");
                                    break;
                                case NULL:
                                    logger.warn("远程buckets中没有Key: {}", new String(key, StandardCharsets.UTF_8));
                                    break;
                            }
                        } else {
                            logger.error("reply = null");
                        }
                    }
                }
            } catch (IOException e) {
                //Storage().get(key), gRPCclient.get(key)抛出Exception时 value均为null, 会进行下一个for循环
                logger.info(e.toString());
                logger.info("查询过程出错！");
            }
            //如果查询到了结果
            if (value != null) {
                break;
            }
        }
        InfosAndClientsRWLock.readLock().unlock();
        return new Pair<>(value, initUncompleted);
    }

    public void sayHello() {
        //对queryServiceClients列表中的各位进行诚挚的问候－－－“您还活着吗？”
        String helloContent = "";
        RPCInterface.HelloRequest request = RPCInterface.HelloRequest.newBuilder()
                .setGreeting(helloContent)
                .build();
        for (int i = 0; i < queryServiceClients.length; i++) {
            if (queryServiceClients[i] != null) {
                RPCInterface.HelloReply reply = queryServiceClients[i].sayHello(request);
                if (reply != null) {
                    logger.info("QueryServiceClient 通信成功, reply.Port: {} <---> queryServiceClient.Port: {}\n" +
                                    "QueryClient workerUUID:[{}]\n",
                            reply.getPort(), queryServiceClients[i].getPort(), workersInfo[i].getWorkerUUID());
                }
            }
        }
    }

    //当两参数有信息不同时，返回true
    private boolean isChanged(final WorkerInfo worker1, final WorkerInfo worker2) {
        return (worker1.getWorkerID() != worker2.getWorkerID()) ||
                (!worker1.getWorkerUUID().equals(worker2.getWorkerUUID())) ||
                (worker1.getQueryServicePort() != worker2.getQueryServicePort());
    }

    public void close() {
        InfosAndClientsRWLock.writeLock().lock();
        for (int i = 0; i < workersInfo.length; i++) {
            if (queryServiceClients[i] != null) {
                try {
                    queryServiceClients[i].shutdown();
                } catch (InterruptedException e) {
                    logger.warn(e.toString());
                    logger.warn("QueryServiceClient关闭时出错, {}", queryServiceClients[i].toString());
                }
            }
            workersInfo[i].clear();
        }
        InfosAndClientsRWLock.writeLock().unlock();
    }
}
