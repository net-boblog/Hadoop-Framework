package com.taomee.bigdata.task.vip;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import java.util.Iterator;
import com.taomee.bigdata.util.GetGameinfo;

public class Vip_year_day_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
{
    //private IntWritable outputValue = new IntWritable();
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

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
		Long time = 0L;
		Integer flag = 1;
		Integer flag_yesterday_all = 0;

        String gameid = key.toString().split("\t")[0];
        String gameinfo = getGameinfo.getValue(gameid);		
        while(values.hasNext()) {
            String time_value = values.next().toString();
			String items[] = time_value.toString().split("\t");
			Long time_temp = Long.valueOf(items[0]);
			Integer flag_temp = Integer.valueOf(items[1]);
			if(time_temp>time) 
			{
				time = time_temp;
				flag = flag_temp;
			}
			if(flag_temp == 2)
			{
				flag_yesterday_all = 1;

			}
        }
		if(flag != 0)
		{
			String tt = String.valueOf(time);
			outputValue.set(tt);
			mos.getCollector("allyearvip" + gameinfo, reporter).collect(key, outputValue);
			if(flag_yesterday_all == 0)
			{
				outputValue.set("1");
				mos.getCollector("newyearvip" + gameinfo, reporter).collect(key, outputValue);
			}
		}

    }
}
