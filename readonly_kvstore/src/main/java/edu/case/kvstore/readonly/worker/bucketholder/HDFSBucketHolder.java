package edu.case.kvstore.readonly.worker.bucketholder;

import edu.case.kvstore.readonly.rpc.RPCInterface;
import edu.case.kvstore.readonly.util.DeleteDirectory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class HDFSBucketHolder implements BucketHolder {
    private int bucketID = -1; //当前BucketHolder负责的bucketID
    private String localDataFold; //文件在本地的位置
    private RocksDB rocksDB; //用于存储具体的key-value数据
    private String hdfsDataFold; //bucket数据文件在HDFS的文件夹路径（每个bucket的数据保存在一个文件中）
    private FileSystem fileSystem;
    private Properties properties;
    private Logger logger;

    @Override
    public void init(Properties properties, int bucketID) throws IOException {
        this.properties = properties;
        this.bucketID = bucketID;
        hdfsDataFold = properties.getProperty("bucketholder.hdfsDataFold");
        localDataFold = "./rocksdb-buckets/" + properties.getProperty("worker.UUID");
        logger = LoggerFactory.getLogger(HDFSBucketHolder.class);
        //尝试初始化hdfs文件，本地RocksDB
        try {
            setUpHDFSFileSystem();
            setUpRocksDB();
            readHdfsBinFileToLocal();
            try {
                this.fileSystem.close();
            } catch (IOException e) {
                logger.warn(e.toString());
                logger.warn("In Method Close, fail to close fileSystem! BucketID: {}", bucketID);
                throw new IOException(e);
            }
        } catch (IOException e) {
            //以上三个方法任意一个出现异常则停止调用，执行这里的catch
            logger.warn(e.toString());
            logger.warn("In Method init, HDFS中没有bucket_{}文件，或打开文件出错! initing结束......Failed", bucketID);
            throw new IOException(e);
        } catch (RocksDBException e) {
            logger.warn(e.toString());
            logger.warn("In Method init, 初始化RocksDB, bucket_{}文件出错! initing结束......Failed", bucketID);
            throw new IOException(e);
        }
    }

    private void setUpHDFSFileSystem() throws IOException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://localhost:9000");
        //conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        //conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // Set HADOOP user
        //System.setProperty("HADOOP_USER_NAME", "hdfs");
        //System.setProperty("hadoop.home.dir", "/");
        //fileSystem = FileSystem.get(conf);
        fileSystem = FileSystem.get(URI.create(hdfsDataFold), conf);
        Path hdfsDataPath = new Path(hdfsDataFold);
        if (!fileSystem.exists(hdfsDataPath)) {
            logger.warn("In Method setUpHDFSFileSystem, HDFS没有文件夹:{}", hdfsDataFold);
            throw new IOException();
        }
    }

    private void setUpRocksDB() throws RocksDBException {
        Options options = new Options();
        options.setCreateIfMissing(true);
        File dir = new File(localDataFold);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RocksDBException("创建本地文件夹" + localDataFold + "失败");
            }
        }
        String rocksDBFilePath = localDataFold + "/bucket_" + bucketID;
        rocksDB = RocksDB.open(options, rocksDBFilePath);
    }

    //读取HDFS上的文本文件并加载到本地，按行读取
    //二进制文件
    private void readHdfsBinFileToLocal() throws IOException, RocksDBException {
        //Path hdfsReadPath = new Path(hdfsDataFold + "/part-" + new DecimalFormat("00000").format(bucketID));
        Path hdfsReadPath = new Path(hdfsDataFold + "/bucket_" + bucketID);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        RPCInterface.KVPair pair;
        while ((pair = RPCInterface.KVPair.parseDelimitedFrom(inputStream)) != null) {
            rocksDB.put(pair.getKey().toByteArray(), pair.getVal().toByteArray());
            //System.out.println(new String(pair.getKey().toByteArray()) + " " + new String(pair.getVal().toByteArray()));
        }
    }

    //读取HDFS上的文本文件并加载到本地，按行读取
    //Text文本文件
    private void readHdfsTextFileToLocal() throws RocksDBException, IOException {
        Path hdfsReadPath = new Path(hdfsDataFold + "/bucket_" + bucketID);
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        String content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        String[] lines = content.split("\n");
        for (String pair : lines) {
            String[] words = pair.split("\t");
            byte[] personVal = words[0].getBytes(StandardCharsets.UTF_8);
            byte[] personName = words[1].getBytes(StandardCharsets.UTF_8);
            rocksDB.put(personName, personVal);
        }
    }

    public void printRocksDBALL() {
        System.out.println();
        System.out.println(rocksDB.getName());
        RocksIterator iter = rocksDB.newIterator();
        //for (iter.seekToFirst(); iter.isValid(); iter.next()) {
          //  System.out.println("iter key:" + new String(iter.key()) + ", iter value:" + new String(iter.value()));
        //}
    }

    @Override
    public void close() throws IOException {
        try {
            //先关闭系统再清楚数据
            try {
                this.rocksDB.close();
                DeleteDirectory.delete(localDataFold); //删除本地RocksDB数据库文件
            } catch (Exception e) {
                logger.warn(e.toString());
                logger.warn("In Method Close, fail to close RocksDB! BucketID: {}", bucketID);
                throw new IOException(e);
            }
        } finally {
            //清除数据
            this.bucketID = -1;
            this.localDataFold = null;
            this.hdfsDataFold = null;
            this.properties = null;
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        //如果key没有对应的value,则返回null
        byte[] res = null;
        if(rocksDB == null){
            throw new IOException("rocksDB = null, holder初始化未完成");
        }
        try {
            res = rocksDB.get(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
        if(res == null){
            logger.warn("key: " + new String(key, StandardCharsets.UTF_8) + " 对应的value值为null----找不到key");
        }
        return res;
    }

    public static void test() throws IOException {
        HDFSBucketHolder hdfsBucketHolder = new HDFSBucketHolder();
        hdfsBucketHolder.init(new Properties(), 3);
        //hdfsBucketHolder.toBinary("/home/hadoop/Desktop/test.txt");
        //hdfsBucketHolder.printRocksDBALL();

        byte[] bytes = hdfsBucketHolder.get("大汉".getBytes());
        String encoded = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(encoded);
        //bytes = hdfsBucketHolder.get(new String("不存在的名字").getBytes());
        //encoded = new String(bytes, StandardCharsets.UTF_8);
        //System.out.println(encoded);
    }

    public static void test2() throws IOException {
        HDFSBucketHolder holder = new HDFSBucketHolder();
        holder.close();
    }

    public static void main(String[] args) throws IOException {
        test();
    }
}
