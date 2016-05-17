package com.taomee.bigdata.task.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import com.taomee.bigdata.lib.*;

import java.io.*;

public class SetSumMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable(1);
    private ReturnCode r = ReturnCode.get();
    private ReturnCodeMgr rOutput;
    private Reporter reporter;

    public void configure(JobConf job) {
        rOutput = new ReturnCodeMgr(job);
    }

    public void close() throws IOException {
        rOutput.close(reporter);
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        this.reporter = reporter;
        String items[] = value.toString().split("\t");
        if(items == null || items.length < 4) {
            r.setCode("E_SETSUM_MAPPER", "items split length < 4");
            return ;
        }
        outputKey.set(String.format("%s\t%s\t%s\t%s",
                    items[0], items[1], items[2], items[3]));
        output.collect(outputKey, outputValue);
    }

}
