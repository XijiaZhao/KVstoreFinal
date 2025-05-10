package edu.case.kvstore.readonly.spark;

import edu.case.kvstore.readonly.rpc.QueryClient;
import edu.case.kvstore.readonly.worker.bucketholder.HDFSBucketHolder;
import edu.case.kvstore.readonly.worker.bucketstorage.SimpleBucketStorage;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class ROKVStoreTest {
    private static Logger logger = LoggerFactory.getLogger(ROKVStoreTest.class);

    public static void test() throws IOException {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        jsc.setLogLevel("WARN");

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
            int getVal = Integer.parseInt(new String(ROKVStore.query(String.valueOf(randomKey).getBytes())));

            if(getVal == randomKey){
                logger.info("queryKey: [{}], getValue: [{}], QueryCount: {}, SUCCESS...\n", randomKey, getVal, i);
            } else{
                logger.warn("queryKey: [{}], getValue: [{}], QueryCount: {}, FAIL...\n", randomKey, getVal, i);
            }
        }
        ROKVStore.shutdown();

        jsc.stop();
        System.exit(0);
    }

    public static void test1_1() throws IOException {
        SparkConf sparkConf = new SparkConf();
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        String bucketFold = "hdfs://xl-slave023:9001/user/wangzk/buckets";

        Properties prop = ROKVStore.generateConfiguration(jsc, bucketFold, SimpleBucketStorage.class.getName(),
                HDFSBucketHolder.class.getName(), QueryClient.class.getName());

        JavaPairRDD<byte[], byte[]> rdd = jsc.textFile("hdfs://xl-slave023:9001/user/wangzk/src/")
                .mapToPair((PairFunction<String, byte[], byte[]>) line ->
                        new Tuple2<>(line.split(" ")[0].getBytes(), line.split(" ")[1].getBytes()));

        ROKVStore.writeKVPairRDDToHDFS(rdd, prop, true);
        ROKVStore.start(jsc, prop);

        int queryNum = 100;
        List<Integer> list = Arrays.asList(new Integer[queryNum]);
        JavaRDD<Integer> queryRDD = jsc.parallelize(list).repartition(queryNum);

        JavaRDD<Long> timeCntRDD = queryRDD.map((Function<Integer, Long>) integer -> {
            long t0 = System.currentTimeMillis();
            int randomKey = (new Random()).nextInt(5000000 - 1);
            int queryRes = Integer.parseInt(new String(ROKVStore.query(String.valueOf(randomKey).getBytes())));
            String res = (queryRes == randomKey) ? "SUCCESS..." : "FAIL...";
            System.out.println(res);
            long t1 = System.currentTimeMillis();
            return t1 - t0;
        });

        long queryCnt = timeCntRDD.count();
        long total_time = timeCntRDD.reduce((Function2<Long, Long, Long>) Long::sum);
        long avg_time = total_time / queryCnt;

        List<Long> timeArray = timeCntRDD.collect();
        List<Long> timeList = new ArrayList<>(timeArray);

        int size = timeList.size();
        Collections.sort(timeList);

        long min_time = timeList.get(0);
        long max_time = timeList.get(timeList.size() - 1);

        long quantile_25 = timeList.get((int) (0.25 * size));
        long quantile_50 = timeList.get((int) (0.50 * size));
        long quantile_75 = timeList.get((int) (0.75 * size));
        long quantile_99 = timeList.get((int) (0.99 * size));

        System.out.println("avg_time: " + avg_time);
        System.out.println("total_time: " + total_time);
        System.out.println("max_time: " + max_time);
        System.out.println("min_time: " + min_time);
        System.out.println("quantile_25: " + quantile_25);
        System.out.println("quantile_50: " + quantile_50);
        System.out.println("quantile_75: " + quantile_75);
        System.out.println("quantile_99: " + quantile_99);
        System.out.println();
        System.out.println("queryCnt: " + queryCnt);
        System.out.println("size: " + size);

        jsc.stop();
        System.exit(0);
    }

    public static void main(String[] args) throws IOException {
        test1_1();
    }
}
