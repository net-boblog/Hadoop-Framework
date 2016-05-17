Hadoop-Framework
======================
Hadoop的mapreduce编程框架，使用者只需要专注于具体业务逻辑即可,该框架封装了mapreduce的具体运行流程，用户将业务逻辑书写后，需要调用shell脚本去执行程序。

***
###                  Author:DMINER
###             E-mail:yanke_shanghai@126.com

======================
使用说明
--------
###一、编写具体逻辑
    在目录【Hadoop-Framework/src/com/taomee/bigdata/task】下编写具体Map 和 Reduce逻辑
	例如：
```
public class Vip_day_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    private Text outputKey = new Text();
    private Text outputValue = new Text();
    private ReturnCode r = ReturnCode.get();
    private ReturnCodeMgr rOutput;
    private Reporter reporter;

    public void configure(JobConf job) {
        rOutput = new ReturnCodeMgr(job);
    }       

    public void close() throws IOException {
        rOutput.close(reporter);
    }       

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter repo
rter) throws IOException
    {
        this.reporter = reporter;
        String items[] = value.toString().split("\t");

        outputKey.set(String.format("%s\t%s\t%s\t%s\t%s",
                    items[0], items[1], items[2], items[3], items[4]));
        outputValue.set(String.format("%s\t%s", items[5],2));
        output.collect(outputKey, outputValue);
    }       

}
```
