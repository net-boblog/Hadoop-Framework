WORKDIR=`dirname "$0"`
WORKDIR=`cd "$WORKDIR"; pwd`

echo workdir = ${WORKDIR}
cd ${WORKDIR}


HADOOP_PATH=/opt/taomee/hadoop/hadoop/bin/
HADOOP_JAR_PATH=${WORKDIR}/stat-bigdata.jar
HADOOP_CONF=${WORKDIR}/hadoop-cluster.xml

BASEDIR=/bigdata/output

##########################################################


RAW_DIR=/bigdata/input
ALL_DIR=${BASEDIR}/all
DAY_DIR=${BASEDIR}/day
WEEK_DIR=${BASEDIR}/week
MONTH_DIR=${BASEDIR}/month
TMP_DIR=${BASEDIR}/tmp
SUM_DIR=${BASEDIR}/sum
BACKUP_DIR=${BASEDIR}/backup

##########################################################

