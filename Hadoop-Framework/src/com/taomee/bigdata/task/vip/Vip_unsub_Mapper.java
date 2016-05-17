package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import com.taomee.bigdata.lib.*;
import com.taomee.bigdata.util.LogAnalyser;
import java.io.*;

public class Vip_unsub_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    protected String stid = null;
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private LogAnalyser logAnalyser = new LogAnalyser();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
         if(logAnalyser.analysis(value.toString()) == ReturnCode.G_OK &&
            (logAnalyser.getValue("_stid_").compareTo("_unsub_") == 0)){
            String game = logAnalyser.getValue(logAnalyser.GAME);
            String platform = logAnalyser.getValue(logAnalyser.PLATFORM);
            String zone = logAnalyser.getValue(logAnalyser.ZONE);
            String server = logAnalyser.getValue(logAnalyser.SERVER);
            String uid = logAnalyser.getValue("_acid_");
            if(game != null &&
                platform != null &&
                zone != null &&
                server != null &&
                uid != null) {
                outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                            game, zone, server, platform, "unsub"));
				
                outputValue.set(String.format("%s",uid));
                output.collect(outputKey, outputValue);
            }
        }
    }

}
