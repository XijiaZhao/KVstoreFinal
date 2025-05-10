package edu.case.kvstore.readonly.util;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class BucketPartitioner {
    private static HashFunction murmur3 = Hashing.murmur3_32();

    public static int getBucketID(byte[] key, int numBucket) {
        return (Math.abs(murmur3.hashBytes(key).hashCode())) % numBucket;
    }
}
