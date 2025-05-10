package edu.case.kvstore.readonly.impl;

import edu.case.kvstore.readonly.master.Master;
import edu.case.kvstore.readonly.rpc.QueryClient;
import edu.case.kvstore.readonly.util.BucketPartitioner;
import edu.case.kvstore.readonly.util.Record;
import edu.case.kvstore.readonly.worker.Worker;
import edu.case.kvstore.readonly.worker.bucketholder.HDFSBucketHolder;
import edu.case.kvstore.readonly.worker.bucketstorage.SimpleBucketStorage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class ROKVStoreImpl {
    private static Master master;
    private static Worker userQueryClient;

    private static JavaRDD<Integer> rdd;
    private static Properties workerConf;

    private static Logger logger = LoggerFactory.getLogger(ROKVStoreImpl.class);

    private static void testuery(Worker aworker, Logger logger) throws InterruptedException {
        Record[] recordList = new Record[3];
        recordList[0] = new Record("东方不败".getBytes(), String.valueOf(2.6788015).getBytes(), 0);
        recordList[1] = new Record("周颠".getBytes(), String.valueOf(3.0444589).getBytes(), 1);
        recordList[2] = new Record("心砚".getBytes(), String.valueOf(4.4911866).getBytes(), 2);
        int index = 0;
        int cnt = 0;
        while (cnt < 100) {
            try {
                byte[] getVal = aworker.query(recordList[index].getKey());
                String res = (Arrays.equals(getVal, recordList[index].getValue())) ? "SUCCESS..." : "FAILED...";
                logger.info("Key: {}, Value: {}, BucketID: {}, Index: {}, Res: {}, ", new String(recordList[index].getKey()),
                        new String(recordList[index].getValue()), recordList[index].getBucket(), index, res);
                logger.info("expect: {}, get: {}\n", new String(getVal), new String(recordList[index].getValue()));
            } catch (IOException e) {
                logger.warn(e.toString());
            }
            index = (index + 1) % 3;
            cnt++;
            Thread.sleep(200);
        }
    }

    private static void testuery_Random10000(Worker aworker, Logger logger, int numBucket) throws InterruptedException {
        int cnt = 0;
        while (cnt < 100) {
            try {
                int randomNum = (new Random()).nextInt(10000 - 1);
                byte[] getVal = aworker.query(String.valueOf(randomNum).getBytes());
                logger.info("expect: {}, get: {}", randomNum, new String(getVal));
                if (Arrays.equals(getVal, String.valueOf(randomNum).getBytes())) {
                    logger.info("Key: {}, Value: {}, BucketID: {}, Res: {}\n", randomNum, randomNum,
                            BucketPartitioner.getBucketID(String.valueOf(randomNum).getBytes(), numBucket), "SUCCESS...");
                } else {
                    logger.error("Key: {}, Value: {}, BucketID: {}, Res: {}\n", randomNum, randomNum,
                            BucketPartitioner.getBucketID(String.valueOf(randomNum).getBytes(), numBucket), "FAILED...");
                }
            } catch (IOException e) {
                logger.warn(e.toString());
            }
            cnt++;
            Thread.sleep(200);
        }
    }

    public static Properties generateConfiguration(JavaSparkContext jsc, String HDFSbucketFold,
                                                   String bucketStorageClassName, String bucketHolderClassName,
                                                   String queryClientClassName) {
        //Worker的数量
        int numExecutor = jsc.getConf().getInt("spark.executor.instances", 4);
        //Bucket的数量
        //int numBucket = numExecutor * bucketRatio;
        int numBucket = 16;
        int numBackups = 2;
        //Master的hostName
        String masterIp = jsc.getConf().get("spark.driver.host");
        //Master的端口号
        int masterPort = 50000 + (int) Thread.currentThread().getId() % 10000;

        Properties prop = new Properties();
        prop.setProperty("master.hostname", String.valueOf(masterIp));
        prop.setProperty("master.port", String.valueOf(masterPort));
        prop.setProperty("num.workers", String.valueOf(numExecutor));
        prop.setProperty("num.buckets", String.valueOf(numBucket));
        prop.setProperty("num.backups", String.valueOf(numBackups));
        prop.setProperty("bucketstorage.classname", bucketStorageClassName);
        prop.setProperty("bucketholder.classname", bucketHolderClassName);
        prop.setProperty("queryclient.classname", queryClientClassName);
        prop.setProperty("bucketholder.hdfsDataFold", HDFSbucketFold);

        logger.info("new configuration: {}", prop.toString());
        ROKVStoreImpl.workerConf = prop;
        return ROKVStoreImpl.workerConf;
    }

    public static void startMaster(Properties prop) {
        int masterPort = Integer.parseInt(prop.getProperty("master.port"));
        int executorNum = Integer.parseInt(prop.getProperty("num.workers"));
        master = Master.startMaster(masterPort, executorNum);
    }

    public static void startWorkers(JavaSparkContext jsc, Properties prop) throws IOException {
        int executorCores = jsc.getConf().getInt("spark.executor.cores", 2);
        int executorNum = Integer.parseInt(prop.getProperty("num.workers"));

        int totalCores = executorNum * executorCores * 4;
        List<Integer> list = Arrays.asList(new Integer[totalCores]);
        Broadcast<Properties> bcProp = jsc.broadcast(prop);

        rdd = jsc.parallelize(list).repartition(totalCores);

        rdd.foreachPartition((VoidFunction<Iterator<Integer>>) integerIterator -> {
            workerConf = bcProp.getValue();
            Worker aWorker = Worker.getSingleton(workerConf, true);
            aWorker.waitSystemInited();
        });
    }

    public static byte[] query(byte[] key) throws IOException {
        userQueryClient = Worker.getSingleton(ROKVStoreImpl.workerConf, false);
        return userQueryClient.query(key);
    }

    public static void shutdownMaster(){
        master.stop();
        master = null;
    }

    public static void shutdownUserQueryClient(){
        userQueryClient.shutdown();
        userQueryClient = null;
    }

    public static void shutdownWorker(){
        rdd.foreachPartition((VoidFunction<Iterator<Integer>>) integerIterator -> {
            Worker.closeSingleton();
        });
    }

    public static void main(String[] args) {
        SparkConf spConf = new SparkConf().setAppName("readonly-kvstore-System");

        JavaSparkContext sc = new JavaSparkContext(spConf);

        int executorNum = sc.getConf().getInt("spark.executor.instances", 3);
        int executorCores = sc.getConf().getInt("spark.executor.cores", 2);

        String masterIp = sc.getConf().get("spark.driver.host");
        System.err.println("spark.executor.instances: " + executorNum);
        System.err.println("masterIp: " + masterIp);

        int masterPort = 50000 + (int) Thread.currentThread().getId() % 10000;

        Properties prop = new Properties();
        prop.setProperty("master.hostname", String.valueOf(masterIp));
        prop.setProperty("master.port", String.valueOf(masterPort));
        prop.setProperty("num.workers", String.valueOf(executorNum));
        prop.setProperty("num.buckets", "12");
        prop.setProperty("num.backups", "2");
        prop.setProperty("bucketstorage.classname", SimpleBucketStorage.class.getName());
        prop.setProperty("bucketholder.classname", HDFSBucketHolder.class.getName());
        prop.setProperty("queryclient.classname", QueryClient.class.getName());
        prop.setProperty("bucketholder.hdfsDataFold", "hdfs://localhost:9000/user/hadoop/buckets");

        Broadcast<Properties> bcProp = sc.broadcast(prop);

        Master.startMaster(masterPort, executorNum);

        int totalCores = executorNum * executorCores * 4;
        List<Integer> list = Arrays.asList(new Integer[totalCores]);
        JavaRDD<Integer> rdd = sc.parallelize(list).repartition(totalCores);

        rdd.foreachPartition((VoidFunction<Iterator<Integer>>) integerIterator -> {
            Properties conf = bcProp.getValue();
            Worker aWorker = Worker.getSingleton(conf, true);
            aWorker.waitSystemInited();
        });

        rdd.foreachPartition((VoidFunction<Iterator<Integer>>) integerIterator -> {
            Properties conf = bcProp.getValue();
            Worker aWorker = Worker.getSingleton(conf, true);
            //testuery(aWorker, LoggerFactory.getLogger(Worker.class));
            testuery_Random10000(aWorker, LoggerFactory.getLogger(Worker.class), Integer.parseInt(prop.getProperty("num.buckets")));
        });
        sc.stop();
        System.exit(0);
    }
}
