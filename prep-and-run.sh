#!/bin/bash

CLEANUP='N'
FORMAT='ALL'
SLEEP_SEC=10
WHITELIST="mapred.reduce.tasks|hive.exec.max.dynamic.partitions.pernode|mapreduce.task.timeout|hive.load.dynamic.partitions.thread|hive.stats.autogather|hive.stats.column.autogather|hive.metastore.dml.events|hive.tez.java.opts|hive.tez.container.size|tez.runtime.io.sort.mb|tez.runtime.unordered.output.buffer.size-mb|tez.grouping.max-size|tez.grouping.min-size|hive.query.name"

#Params
CLUSTER_NAME=$1
AMBARI_USER=$2
AMBARI_PASSWORD=$3
IS_ESP=$4
SSH_USER=$5

# check if the 6 parameter is empty
if [ -z "$6" ]; then
  # if it's empty, set the variable to the default value
  CLEANUP='N'
else
  # if it's not empty, set the variable to the parameter value
  CLEANUP="$6"
fi
echo "CLEANUP is set to $CLEANUP"

if [ -z "$7" ]; then
  FORMAT="ALL"
else
  FORMAT="$7"
fi

echo "FORMAT is set to $FORMAT"

#Constants
echo "Create Directories"

if [ -d "repos" ]; then
  echo "Directory repos exists."
else
  mkdir repos
fi

if [ $IS_ESP = 'Y' ]; then
  sudo chmod a+rwx repos
fi

cd repos

if [ -d "tpcds-hdinsight" ]; then
  echo "Directory tpcds-hdinsight exists."
  rm -r tpcds-hdinsight
fi

echo "Clone tpcds-hdinsight"
git clone https://github.com/mabushaireh/tpcds-hdinsight.git

cd tpcds-hdinsight

config=$(sudo /var/lib/ambari-server/resources/scripts/configs.py -p 8080 -a get -l headnodehost -c hive-site -n $CLUSTER_NAME -k "hive.security.authorization.sqlstd.confwhitelist.append" -u $AMBARI_USER -p "$AMBARI_PASSWORD" | grep $WHITELIST)

if [ -z "$config" ]; then
  echo "update whitelisted configuration for runtime modify"
  sudo /var/lib/ambari-server/resources/scripts/configs.py -p 8080 -a set -l headnodehost -c hive-site -n $CLUSTER_NAME -k "hive.security.authorization.sqlstd.confwhitelist.append" -v $WHITELIST -u $AMBARI_USER -p "$AMBARI_PASSWORD"
  echo "Stop Hive "
  curl -u $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -X PUT -d '{"RequestInfo": {"context" :"STOP HIVE via REST by hive-testBench"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}' http://headnodehost:8080/api/v1/clusters/$CLUSTER_NAME/services/HIVE

  i=0
  sleep $SLEEP_SEC

  while [ $i -le 10 ]; do
    output=$(curl -u $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -X GET http://headnodehost:8080/api/v1/clusters/$CLUSTER_NAME/services/HIVE?fields=ServiceInfo | grep '\"state\" : \"INSTALLED\"')
    echo $output
    if [ -z "$output" ]; then
      echo "Stopping"
      i=$(($i + 1))

      sleep $(($i * $SLEEP_SEC))
    else
      echo "Stopped"
      break
    fi
  done

  sleep $SLEEP_SEC

  echo "Start Hive "
  curl -u $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -X PUT -d '{"RequestInfo": {"context" :"START HIVE via REST by hive-testBench"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}' http://headnodehost:8080/api/v1/clusters/$CLUSTER_NAME/services/HIVE

  i=0
  sleep $SLEEP_SEC

  while [ $i -le 10 ]; do
    output=$(curl -u $AMBARI_USER:$AMBARI_PASSWORD -H "X-Requested-By: ambari" -X GET http://headnodehost:8080/api/v1/clusters/$CLUSTER_NAME/services/HIVE?fields=ServiceInfo | grep '\"state\" : \"STARTED\"')
    echo $output
    if [ -z "$output" ]; then
      echo "Starting"
      i=$(($i + 1))
      sleep $(($i * $SLEEP_SEC))
    else
      echo "Started"
      break
    fi
  done

else

  echo "Whiteliest already applied!"
fi

if [ $IS_ESP = 'Y' ]; then
fi

if [ $CLEANUP = 'Y' ]; then
  echo "Cleanup storage Data!"
  hdfs dfs -rm -f -R /HiveTPCDS/
  hdfs dfs -rm -f -R /tmp/resources
  echo "Copy resources files to hdf tmp folder!"
  hdfs dfs -mkdir /HiveTPCDS
  hdfs dfs -copyFromLocal resources /tmp/resources
  hdfs dfs -ls /tmp/resources

  echo "Generate Data!"
  /usr/bin/hive -n "" -p "" -f TPCDSDataGen.hql -hivevar SCALE=3 -hivevar PARTS=10 -hivevar LOCATION=/HiveTPCDS/ -hivevar TPCHBIN=$(grep -A 1 "fs.defaultFS" /etc/hadoop/conf/core-site.xml | grep -o "abfs[^<]*")/tmp/resources --hivevar QUERY=TPCDSDataGen_$(date '+%Y%m%d_%H%M%S')
  echo "Create External Tables!"
  /usr/bin/hive -n "" -p "" -f ddl/createAllExternalTables.hql -hivevar LOCATION=/HiveTPCDS/ -hivevar DBNAME=tpcds --hivevar QUERY=createAllExternalTables_$(date '+%Y%m%d_%H%M%S')
  echo "Analyze External Tables!"
  /usr/bin/hive -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds --hivevar QUERY=analyze_external_$(date '+%Y%m%d_%H%M%S')
fi

if [[ "$FORMAT" == "ALL" || "$FORMAT" == "Parquet" ]]; then
  echo "Create Parquet Tables!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f ddl/createAllParquetTables.hql -hivevar PARQUETDBNAME=tpcds_parquet -hivevar SOURCE=tpcds --hivevar QUERY=createAllParquetTables_$(date '+%Y%m%d_%H%M%S')
  echo "Analyze Parquet Tables!"
  /usr/bin/hive -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds_parquet --hivevar QUERY=analyze_Parquet_$(date '+%Y%m%d_%H%M%S')

  echo "Run Queries Parquet Tables!"
  for f in queries/*.sql; do for i in {1..1}; do
    STARTTIME="$(date +%s)"
    /usr/bin/hive -i settings.hql -f $f -hivevar ORCDBNAME=tpcds_parquet --hivevar QUERY=$f.$(date '+%Y%m%d_%H%M%S') >$f.run_$i.out 2>&1
    SUCCESS=$?
    ENDTIME="$(date +%s)"
    echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>times_parquet.csv
  done; done

elif [[ "$FORMAT" == "ALL" || "$FORMAT" == "ORC" ]]; then
  echo "Create ORC Tables!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f ddl/createAllORCTables.hql -hivevar ORCDBNAME=tpcds_orc -hivevar SOURCE=tpcds --hivevar QUERY=createAllORCTables_$(date '+%Y%m%d_%H%M%S')
  echo "Analyze ORC Tables!"
  /usr/bin/hive -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds_orc --hivevar QUERY=analyze_ORC_$(date '+%Y%m%d_%H%M%S')

  echo "Run Queries ORC Tables!"
  for f in queries/*.sql; do for i in {1..1}; do
    STARTTIME="$(date +%s)"
    /usr/bin/hive -i settings.hql -f $f -hivevar ORCDBNAME=tpcds_orc --hivevar QUERY=$f.$(date '+%Y%m%d_%H%M%S') >$f.run_$i.out 2>&1
    SUCCESS=$?
    ENDTIME="$(date +%s)"
    echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>times_orc.csv
  done; done
else
  echo "Invalid format"
fi
