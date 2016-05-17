package com.taomee.bigdata.lib;

import java.util.HashMap;
import java.util.Iterator;
import java.lang.reflect.Field;

public class ErrorCode {
    public static int OK = 0;
    public static int CLOSE_EXP = 1;
    public static int CLOSE_NULL = 2;
    public static int LOCAL_FILE_NOT_FOUND = 3;
    public static int LOCAL_IOEXP = 4;
    public static int LOCAL_CLOSE_EXP = 5;
    public static int HDFS_REMOTE_EXP = 6;
    public static int HDFS_NOT_UTF8 = 7;
    public static int HDFS_IOEXP = 8;

    private static HashMap<Integer, String> errorStringMap = null;

    public static String getErrorString(int error) {
        if(errorStringMap == null) {
            createStringMap();
        }
        String s = errorStringMap.get(error);
        if(s == null) {
            return "UnknownError-" + error;
        } else {
            return s;
        }
    }

    private static void createStringMap() {
        if(errorStringMap == null) errorStringMap = new HashMap<Integer, String>();
        try {
            Field[] fields = ErrorCode.class.getFields();
            for(int i=0; i<fields.length; i++) {
                Integer k = fields[i].getInt(null);
                String v = fields[i].getName();
                errorStringMap.put(k, v);
            }
        } catch (java.lang.SecurityException e) { System.out.println(e.getMessage());
        } catch (java.lang.IllegalAccessException e) { System.out.println(e.getMessage());}
    }

    public static void main(String args[]) {
        for(int i=0; i<20; i++) {
            System.out.println(i + " -> " + ErrorCode.getErrorString(i));
        }
    }
}
