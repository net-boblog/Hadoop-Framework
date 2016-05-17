package com.taomee.bigdata.task.common;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;

import java.io.*;
import com.taomee.bigdata.task.common.SourceSetMapper;

public class SourceDIF2Mapper extends SourceSetMapper
{
    public void configure(JobConf job) {
        outputValue.set(2);
        super.configure(job);
    }
}
