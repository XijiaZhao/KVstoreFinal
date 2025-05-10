package edu.case.kvstore.readonly.util;

import java.io.File;

public class DeleteDirectory {
    public static void delete(String Directory){
        File dir = new File(Directory);
        if(!dir.isDirectory()) {
            dir.delete();
            return;
        }
        File[] listFiles = dir.listFiles();
        for(File file : listFiles){
            delete(file.getPath());
        }
        dir.delete();
    }

}