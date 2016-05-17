package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.IOException;
import java.util.HashSet;
import com.taomee.bigdata.lib.ReturnCode;
import com.taomee.bigdata.lib.ReturnCodeMgr;

public class Vip_total_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    private Text outputKey = new Text();
    private Text outputValue = new Text();
	//需要计算的游戏
    private HashSet<Integer> expectGame = new HashSet<Integer>();
    private ReturnCode r = ReturnCode.get();
    private ReturnCodeMgr rOutput;
    private Reporter reporter;

    public void configure(JobConf job) {
        String c = job.get("game");
        if(c == null) {
            throw new RuntimeException("game not configured");
        }
        String items[] = c.split(";");
        for(int i=0; i<items.length; i++) {
            expectGame.add(Integer.valueOf(items[i]));
        }
        rOutput = new ReturnCodeMgr(job);
    }
    public void close() throws IOException {
        rOutput.close(reporter);
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        this.reporter = reporter;
        String items[] = value.toString().split("\t");
        Integer time = Integer.valueOf(items[0]);   //时间
        Integer uid  = Integer.valueOf(items[1]);   //mimi
        Integer game = Integer.valueOf(items[2]);   //gid
        Integer chan = Integer.valueOf(items[3]);   //渠道
        Integer type = Integer.valueOf(items[4]);   //操作类型：0未知，1新增VIP，2续费VIP， 3清理VIP，4减少VIP时间，5取消自动取费，6短信退订
        Integer len  = Integer.valueOf(items[5]);   //包月天数
        Integer vip_type  = Integer.valueOf(items[9]);   //VIP类型

        if(type == 1) {
			//新增VIP
            if(expectGame.contains(game)) {
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", "new")); 
                outputValue.set(String.format("%s", uid));
                output.collect(outputKey, outputValue);
				//新增年费VIP
				if(vip_type == 3){
					outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
								  game, "-1", "-1", "-1", "newyear")); 
					outputValue.set(String.format("%s", uid));
					output.collect(outputKey, outputValue);
				}
            }
        }
		else if(type == 2) {
			//续费VIP
            if(expectGame.contains(game)) {
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", "renew")); 
                outputValue.set(String.format("%s\t1", uid));
                output.collect(outputKey, outputValue);
            }
        }
		else if(type == 3) {
			//销户VIP
            if(expectGame.contains(game)) {
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", "ccacct")); 
                outputValue.set(String.format("%s", uid));
				//销户年费VIP
                output.collect(outputKey, outputValue);
				if(vip_type == 3){
					outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
								  game, "-1", "-1", "-1", "ccacctyear")); 
					outputValue.set(String.format("%s", uid));
					output.collect(outputKey, outputValue);
				}
            }
        }
		else if(type == 5 || type == 6) {
			//退订VIP
            if(expectGame.contains(game)) {
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", "unsub")); 
                outputValue.set(String.format("%s", uid));
                output.collect(outputKey, outputValue);
            }
        }
    }
}
