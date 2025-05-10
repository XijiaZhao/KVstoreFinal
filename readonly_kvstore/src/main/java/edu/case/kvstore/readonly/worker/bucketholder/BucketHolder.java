package edu.case.kvstore.readonly.worker.bucketholder;

import java.io.IOException;
import java.util.Properties;

public interface BucketHolder {
    void init(Properties conf, int bucketID) throws IOException;//配置BucketHolder，并指定bucketID

    void close() throws IOException;// 关闭BucketHolder

    byte[] get(byte[] key) throws IOException;

}
