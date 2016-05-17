package com.taomee.bigdata.task.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import com.taomee.bigdata.task.common.SetMapper;

public class DIF2Mapper extends SetMapper
{
    public void configure(JobConf job) {
        outputValue.set(2);
        super.configure(job);
    }
}
