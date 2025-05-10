package edu.case.kvstore.readonly.util;

//测试用
public class Record {
    private byte[] key = null;
    private byte[] value = null;
    private int bucket = -1;

    public Record(byte[] key, byte[] value, int bucket){
        this.key = key;
        this.value = value;
        this.bucket = bucket;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public int getBucket() {
        return bucket;
    }
}
