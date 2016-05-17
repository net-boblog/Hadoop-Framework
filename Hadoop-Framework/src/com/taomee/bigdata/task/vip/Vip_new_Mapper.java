package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.IOException;
import java.util.HashSet;
import com.taomee.bigdata.lib.ReturnCode;
import com.taomee.bigdata.lib.ReturnCodeMgr;

public class Vip_new_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    private Text outputKey = new Text();
    private Text outputValue = new Text();
	//需要计算的游戏
    private HashSet<Integer> expectGame = new HashSet<Integer>();
	//需要过滤的渠道,包括短信续费渠道
    private HashSet<Integer> ignoreChan = new HashSet<Integer>();
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
        c = job.get("vipchannel");
        if(c == null) {
            throw new RuntimeException("vipchannel not configured");
        }
        items = c.split(";");
        for(int i=0; i<items.length; i++) {
            ignoreChan.add(Integer.valueOf(items[i]));
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

        if(type == 1) {
            if(expectGame.contains(game) && !ignoreChan.contains(chan)) {
                Integer cost = calcValue(game, len);
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", len, "new")); 
                outputValue.set(String.format("%s\t%s\t1", cost, uid));
                output.collect(outputKey, outputValue);
            }
        }
		else if(type == 2) {
            if(expectGame.contains(game) && !ignoreChan.contains(chan)) {
                Integer cost = calcValue(game, len);
				outputKey.set(String.format("%s\t%s\t%s\t%s\t%s\t%s",
                              game, "-1", "-1", "-1", len, "renew")); 
                outputValue.set(String.format("%s\t%s\t1", cost, uid));
                output.collect(outputKey, outputValue);
            }
        }
    }

    private Integer calcValue(int game, int len) {
        //【计算vip的花费】
        //Interval= time_length / 30
        //1.  热血精灵派不打折(gameid=16)   
        //Cost = Interval * 10 * 100 (单位为分)
        //2.  其他游戏
        //若Interval=6：   Cost=5000
        //若Interval=12：  Cost=10000
        //其他：   Cost = Interval * 10 * 100
        Integer value;
        if(len < 30)    return 0;
        len /= 30;
        if(game == 16) {
            value = len * 1000;
        } else {
            if(len == 6) {
                value = 5000;
            } else if(len == 12) {
                value = 10000;
            } else {
                value = len * 1000;
            }
        }
        return value;
    }
}
