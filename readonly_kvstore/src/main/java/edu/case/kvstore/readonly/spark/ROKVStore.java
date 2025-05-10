package edu.case.kvstore.readonly.spark;

import edu.case.kvstore.readonly.rpc.QueryClient;
import edu.case.kvstore.readonly.util.BucketMaker;
import edu.case.kvstore.readonly.worker.Worker;
import edu.case.kvstore.readonly.worker.bucketholder.HDFSBucketHolder;
import edu.case.kvstore.readonly.worker.bucketstorage.SimpleBucketStorage;
import edu.case.kvstore.readonly.impl.ROKVStoreImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.Random;

public class ROKVStore {
    private static Logger logger = LoggerFactory.getLogger(ROKVStore.class);

    public static Properties generateConfiguration(JavaSparkContext jsc, String HDFSbucketFold,
                                                   String bucketStorageClassName, String bucketHolderClassName,
                                                   String queryClientClassName) {
        return ROKVStoreImpl.generateConfiguration(jsc, HDFSbucketFold, bucketStorageClassName,
                bucketHolderClassName, queryClientClassName);
    }

    public static void writeKVPairRDDToHDFS(JavaPairRDD<byte[], byte[]> rdd, Properties conf, boolean overwrite)
            throws IOException {
        int numBucket = Integer.parseInt(conf.getProperty("num.buckets"));
        String HDFSBucketFold = conf.getProperty("bucketholder.hdfsDataFold");

        FileSystem fileSystem = FileSystem.get(URI.create(HDFSBucketFold), new Configuration());
        Path bucketFold = new Path(HDFSBucketFold);
        if (fileSystem.exists(bucketFold)) {
            if (!overwrite) {
                throw new IOException("已存在bucket目录: " + HDFSBucketFold + ", 参数overwrite必须为true!");
            } else {
                logger.info("bucket目录: {} 已存在, 将删除该目录", HDFSBucketFold);
                fileSystem.delete(bucketFold, true);
            }
        }
        BucketMaker.makeBuckets(rdd, fileSystem, numBucket, HDFSBucketFold);
    }

    public static void start(JavaSparkContext jsc, Properties prop) throws IOException {
        //启动Master
        ROKVStoreImpl.startMaster(prop);
        logger.info("master started ...");
        //启动userQueryClient
        Worker.getSingleton(prop, false);
        logger.info("userQueryClient started ...");
        //启动Worker
        ROKVStoreImpl.startWorkers(jsc, prop);
        logger.info("workers started ...");
    }

    public static byte[] query(byte[] key) throws IOException {
        return ROKVStoreImpl.query(key);
    }

    public static void shutdown() throws IOException {
        ROKVStoreImpl.shutdownUserQueryClient();
        ROKVStoreImpl.shutdownWorker();
        ROKVStoreImpl.shutdownMaster();
    }

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String bucketFold = "hdfs://localhost:9000/user/hadoop/buckets";

        Properties prop = ROKVStore.generateConfiguration(jsc, bucketFold, SimpleBucketStorage.class.getName(),
                HDFSBucketHolder.class.getName(), QueryClient.class.getName());

        JavaPairRDD<byte[], byte[]> rdd = jsc.textFile("hdfs://localhost:9000/user/hadoop/src/")
                .mapToPair((PairFunction<String, byte[], byte[]>) line ->
                        new Tuple2<>(line.split(" ")[0].getBytes(), line.split(" ")[1].getBytes()));

        ROKVStore.writeKVPairRDDToHDFS(rdd, prop, true);
        ROKVStore.start(jsc, prop);

        for (int i = 0; i < 300; i++) {
            int randomKey = (new Random()).nextInt(10000 - 1);
            int getVal = Integer.parseInt(new String(query(String.valueOf(randomKey).getBytes())));
            String res = (getVal == randomKey) ? "SUCCESS..." : "FAIL...";
            logger.info("queryKey: [{}], getValue: [{}], QueryCount: {}, {}\n", randomKey, getVal, i, res);
        }
        ROKVStore.shutdown();

        jsc.stop();
        System.exit(0);
    }
}
