package com.taomee.bigdata.task.vip;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import com.taomee.bigdata.task.vip.Vip_stop_Mapper;

public class Vip_logout_system_Mapper extends Vip_stop_Mapper
{
    public void configure(JobConf job) {
        stid = "_ccacct_";
        super.configure(job);
    }
}
