package edu.case.kvstore.readonly.util;

import edu.case.kvstore.readonly.rpc.RPCInterface;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;

//该类已重写
public class WriteToHDFS {
    private static String hdfsDataFlod;
    private static FileSystem fileSystem;
    private static Logger logger = LoggerFactory.getLogger(WriteToHDFS.class);
    public static int numBuckets;

    private static void setUpHDFSFileSystem(String ahdfsDataFold) throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://localhost:9000");
        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // Set HADOOP user
        //System.setProperty("HADOOP_USER_NAME", "hdfs");
        //System.setProperty("hadoop.home.dir", "/");
        //fileSystem = FileSystem.get(conf);
        fileSystem = FileSystem.get(URI.create(ahdfsDataFold), conf);
        System.out.println(fileSystem.getCanonicalServiceName());
        hdfsDataFlod = ahdfsDataFold;
        Path hdfsDataPath = new Path(hdfsDataFlod);
        if (!fileSystem.exists(hdfsDataPath)) {
            fileSystem.mkdirs(hdfsDataPath);
        }
    }

    private static void classifiTheFile(String fileName) throws IOException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        HashFunction murmur3 = Hashing.murmur3_32();

        File dir = new File("rocksdb-buckets");
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new IOException("创建本地文件夹 ./rocksdb-buckets 失败");
            }
        }
        try {
            RocksDB[] rocksDBs = new RocksDB[numBuckets];
            for (int i = 0; i < numBuckets; i++) {
                String localDataPath = "./rocksdb-buckets/bucket_" + String.valueOf(i);
                rocksDBs[i] = RocksDB.open(options, localDataPath);
            }

            BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName));
            String line = bufferedReader.readLine();
            while (line != null) {
                String[] str = line.split(" ");
                byte[] personVal = str[1].getBytes(StandardCharsets.UTF_8);
                byte[] personName = str[0].getBytes(StandardCharsets.UTF_8);
                //String encoded = new String(bytes, StandardCharsets.UTF_8);

                System.out.println(line);
                int bucketID = (Math.abs(murmur3.hashBytes(personName).hashCode())) % numBuckets;

                rocksDBs[bucketID].put(personName, personVal);
                line = bufferedReader.readLine();
            }
            bufferedReader.close();

            for (int i = 0; i < numBuckets; i++) {
                rocksDBs[i].close();
            }
        } catch (IOException e) {
            logger.warn(e.toString());
            logger.warn("fail in classifiTheFile");
            throw new IOException(e);
        } catch (RocksDBException e) {
            logger.warn(e.toString());
            logger.warn("open local rocksDB failed");
            throw new IOException(e);
        }
    }

    private static void localRocksDBToHDFS(String fold) throws IOException {
        int cnt = 0;
        File dir = new File(fold);
        if (!dir.isDirectory()) {
            logger.warn("{} is not a dictionary", fold);
            return;
        }
        File[] files = dir.listFiles();
        Options options = new Options();
        options.setCreateIfMissing(false);
        try {
            for (File file : files) {
                String num = file.getPath().split("bucket_")[1];
                RocksDB rocksDB = RocksDB.open(options, "./rocksdb-buckets/bucket_" + String.valueOf(num));

                FSDataOutputStream output = fileSystem.create(new Path(hdfsDataFlod + "/bucket_" + num));
                RocksIterator iter = rocksDB.newIterator();
                for (iter.seekToFirst(); iter.isValid(); iter.next()) {
                    //System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
                    logger.info("Key: {}, Value: {}, bucketID: {}, cnt: {}", new String(iter.key(), StandardCharsets.UTF_8),
                            new String(iter.value(), StandardCharsets.UTF_8), num, cnt);
                    cnt++;
                    RPCInterface.KVPair pair = RPCInterface.KVPair.newBuilder()
                            .setKey(ByteString.copyFrom(iter.key()))
                            .setVal(ByteString.copyFrom(iter.value()))
                            .build();
                    pair.writeDelimitedTo(output);
                    //output.write(pair.toByteArray());
                }
            }
        } catch (IOException e) {
            logger.warn(e.toString());
            logger.warn("IOException in localRocksDBToHDFS, open hdfs failed");
            throw new IOException(e);
        } catch (RocksDBException e) {
            logger.warn(e.toString());
            logger.warn("open local rocksDB failed");
            throw new IOException(e);
        }
    }

    private static void close() throws IOException {
        fileSystem.close();
    }

    private static void readHDFSFold() throws IOException {
        int cnt = 0;
        FileWriter fileWriter = new FileWriter("KVID2.txt");
        for (int i = 0; i < numBuckets; i++) {
            Path hdfsReadPath = new Path(hdfsDataFlod + "/bucket_" + String.valueOf(i));
            try {
                FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
                RPCInterface.KVPair pair;
                try {
                    while ((pair = RPCInterface.KVPair.parseDelimitedFrom(inputStream)) != null) {
                        //rocksDB.put(pair.getKey().toByteArray(), pair.getVal().toByteArray());
                        System.out.println(pair.getKey().toStringUtf8() + " " + pair.getVal().toStringUtf8() + " " + cnt);
                        fileWriter.write(pair.getKey().toStringUtf8() + " " + pair.getVal().toStringUtf8() + " " + i + "\n");
                        cnt++;
                    }
                    System.out.println();
                } catch (IOException e) {
                    logger.warn(e.toString());
                }
            } catch (IOException e) {
                logger.warn(e.toString());
                logger.warn("In Method readHdfsBinFileToLocal, bucket_{}不存在或打开失败", i);
                throw new IOException(e);
            }
        }
        fileWriter.close();
    }

    private static void writeTestFile(String dst, int numBuckets) throws IOException {
        FileWriter writer = new FileWriter(dst);
        for (int i = 0; i < 10000; i++){
            writer.write(i + " " + i + " " + BucketPartitioner.getBucketID(String.valueOf(i).getBytes(), numBuckets) + "\n");
        }
        writer.close();
    }

    public static void main(String[] args) throws IOException {
        numBuckets = 12;
        writeTestFile("test10000.txt", numBuckets);
    }
}

