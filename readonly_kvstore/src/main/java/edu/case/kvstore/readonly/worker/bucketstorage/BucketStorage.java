package edu.case.kvstore.readonly.worker.bucketstorage;

import java.io.IOException;
import java.util.Properties;

public interface BucketStorage {

    void init(Properties conf, int workerID) throws IOException; //配置BucketStorage，指定workerID

    void close() throws IOException; // 关闭BucketStorage

    byte[] get(byte[] key) throws IOException; // 读取BucketStorage保存的KV对
}
