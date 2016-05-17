package com.taomee.bigdata.lib;

import com.taomee.bigdata.lib.ReturnCode;

import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.lang.Thread;
import java.lang.Class;
import java.lang.reflect.Method;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class ReturnCodeMgr
{
    private JobConf conf;
    private ReturnCode rCode;
    private HashMap<String, String> type;
    private MultipleOutputs mos = null;
    private Text outputKey = new Text();
    private NullWritable outputValue = NullWritable.get();

    public ReturnCodeMgr(JobConf conf) {
        this.conf = conf;
        rCode = ReturnCode.get();
        type = rCode.getType();
        Iterator<String> it = type.keySet().iterator();
        while(it.hasNext()) {
            try {
                MultipleOutputs.addNamedOutput(
                        conf, it.next(),
                        Class.forName("org.apache.hadoop.mapred.TextOutputFormat").asSubclass(OutputFormat.class),
                        Class.forName("org.apache.hadoop.io.Text").asSubclass(WritableComparable.class),
                        Class.forName("org.apache.hadoop.io.NullWritable").asSubclass(Writable.class));
            } catch (java.lang.ClassNotFoundException e) {
                System.out.println(e.getMessage());
            } catch (java.lang.IllegalArgumentException e) { }
        }
        mos = new MultipleOutputs(conf);
    }

    public MultipleOutputs getMos() { return mos; }

    public void close(Reporter reporter) {
        Iterator<String> it = type.keySet().iterator();
        while(it.hasNext()) {
            Method m = null;
            String key = it.next();
            try {
                m = rCode.getClass().getMethod(type.get(key));
                ArrayList<String> a = (ArrayList<String>)m.invoke(rCode);
                for(int i=0; i<a.size(); i++) {
                    try {
                        outputKey.set(a.get(i));
                        mos.getCollector(key, reporter).collect(outputKey, outputValue);
                    } catch (java.io.IOException e) {
                        System.err.println(e.getMessage());
                    }
                }
            } catch (java.lang.NoSuchMethodException e) {
                System.err.println(e.getMessage());
            } catch (java.lang.IllegalAccessException e) {
                System.err.println(e.getMessage());
            } catch (java.lang.reflect.InvocationTargetException e) {
                System.err.println(e.getMessage());
            }
        }
        try {
            mos.close();
        } catch (java.io.IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
