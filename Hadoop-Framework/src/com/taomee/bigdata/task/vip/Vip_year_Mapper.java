package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import com.taomee.bigdata.task.vip.Vip_live_Mapper;

public class Vip_year_Mapper extends Vip_live_Mapper
{
    public void configure(JobConf job) {
        stid = "_yearvip_";
        super.configure(job);
    }
}
