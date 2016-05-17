package com.taomee.bigdata.lib;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import sun.management.ManagementFactory;

public class Logger {
    class LoggerFile {
        public BufferedWriter writer;
        public int day;
        public int length = 0;
        public String prex;
        public int index = 0;
    }

    private final int DEBUG = 0;
    private final int ERROR = 1;
    private final int MAX_LENGTH = 33554432;//32M

    private static SimpleDateFormat format = new SimpleDateFormat();
    private static Logger log = null;
    private static int count = 0;
    private static int pid;
    private LoggerFile debugLoggerFile = new LoggerFile();
    private LoggerFile errorLoggerFile = new LoggerFile();
    private Logger() {
        debugLoggerFile.prex = "debug";
        errorLoggerFile.prex = "error";
        format.applyPattern("HH:mm:ss");
        pid = Integer.valueOf(ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        (new File("log")).mkdirs();
    }

    public static Logger getInstance() {
        if(log == null) {
            log = new Logger();
        }
        count ++;
        return log;
    }

    public void DEBUG_LOG(String s) {
       log(s, DEBUG, new Date());
    }

    public void ERROR_LOG(String s) {
       log(s, ERROR, new Date());
    }
    
    public void EXCEPTION_LOG(Exception e) {
        StackTraceElement[] s = e.getStackTrace();
        Date d = new Date();
        log(e.getClass().getName() + ": " + e.getMessage(), ERROR, d);
        for(int i=0; i<s.length; i++) {
            log("\t at " + s[i].toString(), ERROR, d);
        }
    }

    private void log(String s, int level, Date d) {
        LoggerFile f;
        switch(level) {
            case ERROR:
                f = getErrorFile(d);
                break;
            case DEBUG:
                f = getDebugFile(d);
                break;
            default:
                return;
        }
        if(f.writer != null) {
            try {
                String str = String.format("[%s][%05d]%s\n", format.format(d), pid, s);
                f.writer.write(str);
                f.writer.flush();
                f.length += str.length();
            } catch (IOException e) {

            }
        }
    }

    private LoggerFile getErrorFile(Date d) {
        return getFile(errorLoggerFile, d);
    }

    private LoggerFile getDebugFile(Date d) {
        return getFile(debugLoggerFile, d);
    }

    private BufferedWriter closeWriter(BufferedWriter writer) {
        try {
            if(writer != null) {
                writer.close();     
                return null;
            }  
        } catch (IOException e) {
            e.printStackTrace();
            return writer;
        }
        return writer;
    }

    private LoggerFile getFile(LoggerFile lf, Date d) {
        int day = Integer.valueOf(DateUtils.dateToString(d));
        if(day != lf.day || lf.length >= MAX_LENGTH) {
            lf.writer = closeWriter(lf.writer);
            lf.length = 0;
        }
        if(lf.writer == null) {
            lf.day = day;
            try {
                lf.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(String.format("log/%s_%s_%08d", lf.prex, lf.day, ++lf.index), true)));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return lf;
    }
}
