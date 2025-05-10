package edu.case.kvstore.readonly.util;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class UUIDGenerator {
    public static String generateUUID(String hostName, boolean active) {
        if(!active){
            return "LeaveForUserToQuery";
        }
        String signature = hostName + "-" + Thread.currentThread().getId() + "@" + System.currentTimeMillis();
        byte[] bytes = signature.getBytes(StandardCharsets.UTF_8);
        return UUID.nameUUIDFromBytes(bytes).toString();
    }
}
