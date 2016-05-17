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
        -addInput ${ALL_DIR}/$yesterday/dayvip/part-*,com.taomee.bigdata.task.vip.Vip_day_Mapper \
        -addInput ${DAY_DIR}/$date/basic/buyvip-*,com.taomee.bigdata.task.vip.Vip_login_Mapper \
        -addInput ${DAY_DIR}/$date/basic/ccacct-*,com.taomee.bigdata.task.vip.Vip_logout_system_Mapper \
        -reducerClass com.taomee.bigdata.task.vip.Vip_day_Reducer \
        -output ${ALL_DIR}/$date/dayvip

