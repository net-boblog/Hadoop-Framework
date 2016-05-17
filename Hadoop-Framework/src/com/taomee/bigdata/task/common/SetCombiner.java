package com.taomee.bigdata.task.common;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;

public class SetCombiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable outputValue = new IntWritable();
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
        if(values.hasNext()) {
            output.collect(key, values.next());
        }
    }
}
