package com.taomee.bigdata.task.vip;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;
import com.taomee.bigdata.util.GetGameinfo;

public class Vip_day_sum_Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable outputValue = new IntWritable();
    private GetGameinfo getGameinfo  = GetGameinfo.getInstance();
	private MultipleOutputs mos = null;

	public void configure(JobConf job) {
		mos = new MultipleOutputs(job);
		getGameinfo.config(job);
	}

	public void close() throws IOException {
		mos.close();
	}

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
    {
	    Integer sum = 0;
        String gameid = key.toString().split("\t")[0];
		String gameinfo = getGameinfo.getValue(gameid);
	    while(values.hasNext())
	    {
		    sum += values.next().get();
	    }
	    outputValue.set(sum);
		mos.getCollector("part" + gameinfo, reporter).collect(key, outputValue);
    }
}
