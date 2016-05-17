package com.taomee.bigdata.task.common;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;
import java.util.HashSet;
import com.taomee.bigdata.util.GetGameinfo;

public class SetReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, NullWritable>
{
    private NullWritable outputValue = NullWritable.get();
    private Text outputKey = new Text();
    protected Integer ncount = 0;
    private HashSet<Integer> values = new HashSet<Integer>();
	private GetGameinfo getGameinfo  = GetGameinfo.getInstance();
	private MultipleOutputs mos = null;

    public void configure(JobConf job) {
        String ncount = job.get("ncount");
        if(ncount != null) {
            this.ncount = Integer.valueOf(ncount);
        }
		mos = new MultipleOutputs(job);
		getGameinfo.config(job);
    }
    public void close() throws IOException {
		mos.close();
	}

    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException
    {
        this.values.clear();
        String gameid = key.toString().split("\t")[0];
		String gameinfo = getGameinfo.getValue(gameid);
        while(values.hasNext()) {
            this.values.add(values.next().get());
        }
        Integer n = 0;
        Iterator<Integer> it = this.values.iterator();
        while(it.hasNext()) {
            n += it.next();
        }
        if(n.equals(ncount)) {
			mos.getCollector("part" + gameinfo, reporter).collect(key, outputValue);
        }
    }
}
