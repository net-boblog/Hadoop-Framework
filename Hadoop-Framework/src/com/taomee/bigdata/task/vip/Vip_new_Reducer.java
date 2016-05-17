package com.taomee.bigdata.task.vip;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;
import java.util.HashSet;
import com.taomee.bigdata.lib.*;
import com.taomee.bigdata.util.GetGameinfo;

public class Vip_new_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
	private Text outputKey = new Text();
    private HashSet<String> uids = new HashSet<String>();
    private ReturnCode r = ReturnCode.get();
    private ReturnCodeMgr rOutput;
	private Reporter reporter;
    private MultipleOutputs mos = null;
	private GetGameinfo getGameinfo  = GetGameinfo.getInstance();

    public void configure(JobConf job) {
        rOutput = new ReturnCodeMgr(job);
        mos = rOutput.getMos();
		getGameinfo.config(job);
    }

    public void close() throws IOException {
        rOutput.close(reporter);
		mos.close();
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        this.reporter = reporter;
        double sum = 0.0f;
        int cnt = 0;
        uids.clear();
		String keys[] = key.toString().split("\t");
        String gameid = keys[0];
		String gameinfo = getGameinfo.getValue(gameid);
		String type = keys[5];
        while(values.hasNext()) {
            String items[] = values.next().toString().split("\t");
            sum += Double.valueOf(items[0]);
            uids.add(items[1]);
            cnt += Integer.valueOf(items[2]);
        }
		outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",keys[0],keys[1],keys[2],keys[3],keys[4]));
		if(type.compareTo("new") == 0){
			mos.getCollector("ucountnew" + gameinfo, reporter).collect(outputKey, new IntWritable(uids.size()));
			mos.getCollector("countnew" + gameinfo, reporter).collect(outputKey, new IntWritable(cnt));
			mos.getCollector("amtnew" + gameinfo, reporter).collect(outputKey, new DoubleWritable(sum/100.0f));
		}
		else if(type.compareTo("renew") == 0){
			mos.getCollector("ucountrenew" + gameinfo, reporter).collect(outputKey, new IntWritable(uids.size()));
			mos.getCollector("countrenew" + gameinfo, reporter).collect(outputKey, new IntWritable(cnt));
			mos.getCollector("amtrenew" + gameinfo, reporter).collect(outputKey, new DoubleWritable(sum/100.0f));
		}
    }
}

