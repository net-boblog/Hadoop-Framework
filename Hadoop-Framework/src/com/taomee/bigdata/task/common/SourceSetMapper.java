package com.taomee.bigdata.task.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import com.taomee.bigdata.lib.*;
import com.taomee.bigdata.util.LogAnalyser;
import java.io.*;

public class SourceSetMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text outputKey = new Text();
    protected IntWritable outputValue = new IntWritable(0);
    private ReturnCode r = ReturnCode.get();
    private ReturnCodeMgr rOutput;
    private Reporter reporter;
    private LogAnalyser logAnalyser = new LogAnalyser();
    private String stid = null;

    public void configure(JobConf job) {
        stid = job.get("stid");
        if(stid == null) {
            throw new RuntimeException("stid not configured");
        }
        rOutput = new ReturnCodeMgr(job);
    }

    public void close() throws IOException {
        rOutput.close(reporter);
    }


    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        this.reporter = reporter;
        if(logAnalyser.analysis(value.toString()) == ReturnCode.G_OK &&
            (logAnalyser.getValue("_stid_").compareTo(stid) == 0)) {
            String game = logAnalyser.getValue(logAnalyser.GAME);
            String platform = logAnalyser.getValue(logAnalyser.PLATFORM);
            String zone = logAnalyser.getValue(logAnalyser.ZONE);
            String server = logAnalyser.getValue(logAnalyser.SERVER);
            String uid = logAnalyser.getAPid();
            if(game != null &&
                platform != null &&
                zone != null &&
                server != null &&
                uid != null) {
                outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                            game, zone, server, platform, uid));
                output.collect(outputKey, outputValue);
            }
        }
    }

}
