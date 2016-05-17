package com.taomee.bigdata.task.common;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;
import java.util.HashSet;
import com.taomee.bigdata.util.GetGameinfo;

public class PercentReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text>
{
    private Text outputValue = new Text();
    private GetGameinfo getGameinfo  = GetGameinfo.getInstance();
	private MultipleOutputs mos = null;

	public void configure(JobConf job) {
		mos = new MultipleOutputs(job);
		getGameinfo.config(job);
	}

	public void close() throws IOException {
		mos.close();
	}

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        int a = 0;
        int b = 0;
        String gameid = key.toString().split("\t")[0];
		String gameinfo = getGameinfo.getValue(gameid);
        while(values.hasNext()) {
            int i = values.next().get();
            if(i == 0) {
                a ++;
            } else {
                b ++;
            }
        }
        if(a > b) {
            int c = a;
            a = b;
            b = c;
        }
        if(b == 0 || a == 0)    return;
        double p = 0.0;
        p = (a+0.0)/b;
        outputValue.set(String.format("%.8f", p));
		mos.getCollector("part" + gameinfo, reporter).collect(key, outputValue);
    }
}
