package com.taomee.bigdata.task.common;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;

public class SetSumCombiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable outputValue = new IntWritable();


    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        int n = 0;
        while(values.hasNext()) {
            n += values.next().get();
        }
        outputValue.set(n);
		output.collect(key,outputValue);
    }
}
