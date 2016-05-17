package com.taomee.bigdata.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

public class ModifyTime extends Configured implements Tool
{
    public int run(String[] args) throws IOException
    {
        Configuration conf = new Configuration();  
        FileSystem hdfs = FileSystem.get(conf);  

        int i=0;
        for(i=0; i<args.length; i++) {
            if(args[i].compareTo("-path") == 0) break;
        }
        for(int j=i+1; j<args.length; j++) {
            Path dst = new Path(args[j]);  

            FileStatus files[] = hdfs.listStatus(dst);  
            for (FileStatus file : files) {

                System.out.println(file.getPath() + "\t"  
                        + file.getModificationTime());  

                System.out.println(file.getPath() + "\t"  
                        + new Date(file.getModificationTime()));  


            }  
        }
        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new ModifyTime(), args);
    }
}
