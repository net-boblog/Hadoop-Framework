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
###二、编译
    在目录【Hadoop-Framework/src】下更改makefile文件，然后make（makefile中目录需要更改为自己的环境目录）
```
WORKDIR=../
OBJDIR=$(WORKDIR)/obj
SRCDIR=${WORKDIR}/src
SRCS=$(shell find $(SRCDIR) -name '*.java')
OBJS=$(patsubst %.java,%.class, $(subst $(SRCDIR),$(OBJDIR),$(SRCS)))
#HADOOP_PATH=此处为hadoop安装路径
HADOOP_CLASSPATH=$(shell ${HADOOP_PATH}hadoop classpath)
TARGET=$(WORKDIR)/stat-bigdata.jar

all: $(OBJS)
    cd $(OBJDIR) && cp $(SRCDIR)/init.properties $(OBJDIR)/init.properties && jar cvf $(TARGET) ini
t.properties com && cp $(TARGET) $(SRCDIR)/stat-bigdata.jar

$(OBJDIR)/%.class: $(SRCDIR)/%.java
    javac -encoding utf-8 -classpath $(HADOOP_CLASSPATH):./ $< -d ../obj

clean:
    rm $(OBJS)
```
###三、编写执行脚本
    以上均可在开发机上编写编译，编译后得到jar包后，可以拿到线上hadoop环境去执行，执行时需要有执行脚本，里面配置了执行参数
```
export LANG=en_US.UTF-8
WORKDIR=`dirname $0`
WORKDIR=`cd $WORKDIR && pwd`
cd $WORKDIR
echo workdir = $WORKDIR

source config.sh

date=$1

if [[ $date == "" ]]; then
    echo invalid param: date
    exit
fi

yesterday=`date -d "$date -1 day" +%Y%m%d`

${HADOOP_PATH}hadoop jar ${HADOOP_JAR_PATH} \
        com.taomee.bigdata.driver.MultipleInputsJobDriver \
        -D mapred.output.compress=true \
        -D mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec \
        -D mapred.reduce.tasks=4 \
        -conf ${HADOOP_CONF} \
        -jobName "vip day $date" \
        -outKey org.apache.hadoop.io.Text \
        -outValue org.apache.hadoop.io.Text \
        -inFormat org.apache.hadoop.mapred.TextInputFormat \
        -outFormat org.apache.hadoop.mapred.TextOutputFormat \
        -addInput ${ALL_DIR}/$yesterday/dayvip/part*,com.taomee.bigdata.task.vip.Vip_day_Mapper \
        -addInput ${DAY_DIR}/$date/basic/buyvip*,com.taomee.bigdata.task.vip.Vip_login_Mapper \
        -addInput ${DAY_DIR}/$date/basic/ccacct*,com.taomee.bigdata.task.vip.Vip_logout_system_Mapper \
        -reducerClass com.taomee.bigdata.task.vip.Vip_day_Reducer \
        -output ${ALL_DIR}/$date/dayvip
```

