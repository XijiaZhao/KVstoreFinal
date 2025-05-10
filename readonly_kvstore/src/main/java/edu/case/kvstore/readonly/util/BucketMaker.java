package edu.case.kvstore.readonly.util;

import edu.case.kvstore.readonly.rpc.RPCInterface;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

public class BucketMaker {
    public static void makeBuckets(JavaPairRDD<byte[], byte[]> rdd, FileSystem fileSystem, int numBucket,
                                   String HDFSBucketFold) {
        RDDBucketPartition partitioner = new RDDBucketPartition(numBucket);
        rdd = rdd.partitionBy(partitioner);
        JavaRDD<Integer> cntRDD = rdd.mapPartitionsWithIndex((index, itr) -> {
            FSDataOutputStream output = FileSystem.get(URI.create(HDFSBucketFold), new Configuration())
                    .create(new Path(HDFSBucketFold + "/bucket_" + index));
            ArrayList<Integer> cnt = new ArrayList<>();
            int counter = 0;
            while (itr.hasNext()) {
                Tuple2<byte[], byte[]> tuple2 = itr.next();
                RPCInterface.KVPair pair = RPCInterface.KVPair.newBuilder()
                        .setKey(ByteString.copyFrom(tuple2._1))
                        .setVal(ByteString.copyFrom(tuple2._2))
                        .build();
                //System.out.println(new String(pair.getKey().toByteArray()) + " " + new String(pair.getVal().toByteArray()));
                counter++;
                pair.writeDelimitedTo(output);
            }
            output.close();
            cnt.add(counter);
            return cnt.iterator();
        }, false);
        cntRDD.count();
    }

    public static void main(String[] args) throws IOException {
        String srcFold = "hdfs://localhost:9000/user/hadoop/src/";
        String dstFold = "hdfs://localhost:9000/user/hadoop/buckets/";
        int numBucket = 12;

        SparkConf sparkConf = new SparkConf().setAppName("make-buckets");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<byte[], byte[]> rdd = jsc.textFile(srcFold)
                .mapToPair((PairFunction<String, byte[], byte[]>) line ->
                        new Tuple2<>(line.split(" ")[0].getBytes(), line.split(" ")[1].getBytes()));

        FileSystem fileSystem = FileSystem.get(URI.create(dstFold), new Configuration());
        makeBuckets(rdd, fileSystem, numBucket, dstFold);
    }

    static class RDDBucketPartition extends org.apache.spark.Partitioner {
        int numPartition;

        public RDDBucketPartition(int numPartition) {
            this.numPartition = numPartition;
        }

        @Override
        public int numPartitions() {
            return numPartition;
        }

        @Override
        public int getPartition(Object o) {
            return BucketPartitioner.getBucketID((byte[]) o, numPartition);
        }
    }
}
