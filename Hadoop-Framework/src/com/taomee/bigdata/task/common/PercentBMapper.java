package com.taomee.bigdata.task.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;

public class PercentBMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
{
    private Text outputKey = new Text();
    protected IntWritable outputValue = new IntWritable(1);

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        String items[] = value.toString().split("\t");
        outputKey.set(String.format("%s\t%s\t%s\t%s",
                items[0], items[1], items[2], items[3]));
        output.collect(outputKey, outputValue);
    }

}
