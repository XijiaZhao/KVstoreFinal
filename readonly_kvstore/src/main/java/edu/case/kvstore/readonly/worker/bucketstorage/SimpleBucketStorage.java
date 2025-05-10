package edu.case.kvstore.readonly.worker.bucketstorage;

import edu.case.kvstore.readonly.rpc.QueryClient;
import edu.case.kvstore.readonly.util.BucketPartitioner;
import edu.case.kvstore.readonly.worker.bucketholder.BucketHolder;
import edu.case.kvstore.readonly.worker.bucketholder.HDFSBucketHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SimpleBucketStorage implements BucketStorage {
    protected String bucketHolderClassName;// BucketHolder的类名
    protected int numWorkers;  // Workers的总数N
    protected int numBuckets; // Bucket的总数B
    protected int numBackups;
    protected int workerID; // 当前BucketStorage对应的workerID
    protected BucketHolder[] buckets; // 当前Worker负责的bucket列表
    protected ReadWriteLock rwLock; // 读写锁，用于控制多线程并发访问
    protected Properties conf; //用于存储配置信息，该信息用于初始化BucketHolder

    protected int[] bucketIDList;//用于存储当前bucketStorage下的buckets负责的bucketsID
    private Logger logger;

    @Override
    public void init(Properties conf, int workerID) throws IOException {
        this.bucketHolderClassName = conf.getProperty("bucketholder.classname");
        this.numWorkers = Integer.parseInt(conf.getProperty("num.workers"));
        this.numBuckets = Integer.parseInt(conf.getProperty("num.buckets"));
        this.numBackups = Integer.parseInt(conf.getProperty("num.backups"));
        this.workerID = workerID;
        this.rwLock = new ReentrantReadWriteLock();
        this.conf = conf;

        logger = LoggerFactory.getLogger(SimpleBucketStorage.class);
        //计算负责的bucketID，附带备份
        bucketIDList = bucketIDListWithBackup();
        logger.info("bucketIDList: {}\n", Arrays.toString(bucketIDList));
        //初始化bucketHolder列表
        //buckets数组的长度就是Bucket的个数
        //如果当前worker不负责相应的bucket，则对应位置为null，否则为有效的BucketHolder对象
        buckets = new BucketHolder[numBuckets];
        try {
            Class cla = Class.forName(bucketHolderClassName);
            for (int bucketID : bucketIDList) {
                buckets[bucketID] = (BucketHolder) cla.newInstance();
                //buckerHolder的init会throw Exception，遇到错误停止执行
                buckets[bucketID].init(conf, bucketID);
            }
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException | IOException e) {
            logger.warn(e.toString());
            logger.warn("In Method init, 初始化Storage出错, FAIL");
            for (BucketHolder bucketHolder : buckets) {
                if (bucketHolder != null) {
                    //bucketHolder不会throw Exception，放心处理
                    bucketHolder.close();
                }
            }
            buckets = null;
            throw new IOException(e);
        }
        for (BucketHolder holder : buckets) {
            if (holder != null)
                ((HDFSBucketHolder) holder).printRocksDBALL();
        }
        logger.info("初始化Storage结束, SUCCESS");
    }

    //计算WorkerID的buckets，附带备份
    private int[] bucketIDListWithBackup() {
        Set<Integer> set = new HashSet<>();
        for (int i = 0; i < numBackups; i++) {
            int[] arr = bucketIDList(i);
            for (int id : arr) {
                set.add(id);
            }
        }
        Integer[] bucketIDList = new Integer[set.size()];
        set.toArray(bucketIDList);
        return Arrays.stream(bucketIDList).mapToInt(Integer::intValue).toArray();
    }

    //计算单个WorkerID的buckets
    private int[] bucketIDList(int offset) {
        int offsetWorkerID = workerID - offset;
        if (offsetWorkerID < 0) {
            offsetWorkerID += numWorkers;
        }
        //第i个bucket由第i % N个Worker进程负责提供查询服务
        int quotient = numBuckets / numWorkers;
        int numOfBuckets = quotient + ((offsetWorkerID <= (numBuckets - quotient * numWorkers - 1)) ? 1 : 0);
        bucketIDList = new int[numOfBuckets];
        int cnt = offsetWorkerID;
        for (int i = 0; i < bucketIDList.length; i++) {
            bucketIDList[i] = cnt;
            cnt += numWorkers;
        }
        return bucketIDList;
    }

    @Override
    public void close() throws IOException {
        rwLock.writeLock().lock(); // 加写锁
        this.bucketHolderClassName = null;
        this.numWorkers = -1;
        this.workerID = -1;
        //依次调用buckets列表中BucketHolder对象的close方法
        //如果中间抛出异常，则打印WARNING的信息，然后继续处理直到所有BucketHolder关闭
        //注意这里的bucketHolder.close()虽然有可能出错，但在close()方法中已经捕获到了异常，并且打印了Warning
        //所以这里单个buckerHolder即使close出错，其他的bucketHolder也会继续close
        if (buckets != null) {
            for (BucketHolder bucketHolder : buckets) {
                if (bucketHolder != null) {
                    //注意Holder不抛出异常，所以这个close方法不会捕获到，也就不会抛出异常
                    //这个问题也影响到Worker的startNewService(),closeExistingService()
                    //!**************************
                    bucketHolder.close();
                }
            }
            buckets = null;
        }
        rwLock.writeLock().unlock(); // 释放写锁
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        //MurmurHash3_32方法，计算key对应的bucket ID
        //HashFunction murmur3 = Hashing.murmur3_32();
        //int targetBucketID = Math.abs(murmur3.hashBytes(key).hashCode()) % numBuckets;
        int targetBucketID = BucketPartitioner.getBucketID(key, numBuckets);
        int targetWorkerID = targetBucketID % numWorkers;

        //先对targetBucketID做检查
        //1.检查当前worker的状态
        if (workerID < 0) {
            throw new IOException("Invalid bucketID: " + targetBucketID + " with workerID: " + workerID +
                    "----workerID < 0");
        }
        //2.这个这个bucketID是由当前WorkerID负责的？
        boolean localBackup = false;
        for (int i = 0; i < numBackups; i++) {
            if (workerID == (targetWorkerID + i) % numWorkers) {
                localBackup = true;
                break;
            }
        }
        if (!localBackup) {
            throw new IOException("Invalid bucketID: " + targetBucketID + " with workerID: " + workerID +
                    "----该bucketID不由当前workerID负责");
        }
        rwLock.readLock().lock();
        //3.targetBucketID的bucket初始化完成了吗？
        //************************************
        // 初始化到一半就读取会发生什么？ 会报错...
        if (buckets[targetBucketID] == null) {
            rwLock.readLock().unlock();
            throw new IOException("Cannot get corresponding bucket : " + targetBucketID + " with workerID: " +
                    workerID + "----目标bucket为null");
        }

        //正式读取
        byte[] val = null;
        try {
            val = buckets[targetBucketID].get(key);
        } catch (IOException e) {
            rwLock.readLock().unlock();
            logger.warn("In Method get, buckets get 过程出错");
            throw e;
        }
        rwLock.readLock().unlock();
        return val;
    }

    private static void testBucketIDList() {
        SimpleBucketStorage simpleBucketStorage = new SimpleBucketStorage();
        simpleBucketStorage.numBuckets = 12;
        simpleBucketStorage.numWorkers = 5;
        simpleBucketStorage.workerID = 1;
        System.out.println(Arrays.toString(simpleBucketStorage.bucketIDListWithBackup()));
    }

    private static void testSimpleBucketStorage() throws IOException {
        SimpleBucketStorage storage = new SimpleBucketStorage();
        Properties conf = new Properties();
        conf.setProperty("master.hostname", "127.0.0.1");
        conf.setProperty("master.port", "50051");
        conf.setProperty("num.workers", "5");
        conf.setProperty("num.buckets", "12");
        conf.setProperty("num.backups", "3");
        conf.setProperty("bucketstorage.classname", SimpleBucketStorage.class.getName());
        conf.setProperty("bucketholder.classname", HDFSBucketHolder.class.getName());

        conf.setProperty("bucketholder.hdfsDataFold", "hdfs://localhost:9000/user/hadoop/buckets");
        conf.setProperty("queryclient.classname", QueryClient.class.getName());

        storage.init(conf, 1);
        //for (BucketHolder holder : storage.buckets) {
        //((HDFSBucketHolder) holder).printRocksDBALL();
        //}
        String encoded = new String(storage.get("余沧海".getBytes()), StandardCharsets.UTF_8);
        System.out.println(encoded);
        encoded = new String(storage.get("不存在的名字".getBytes()), StandardCharsets.UTF_8);
        System.out.println(encoded);
        storage.close();
        System.out.println();
    }

    public static void main(String[] args) throws IOException {
        testSimpleBucketStorage();
    }
}
