package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import com.taomee.bigdata.task.vip.Vip_live_Mapper;

public class Vip_login_Mapper extends Vip_live_Mapper
{
    public void configure(JobConf job) {
        stid = "_buyvip_";
        super.configure(job);
    }
}
