#!/bin/bash

#Variables
CLUSTER_NAME=$1
AMBARI_USER=$2
AMBARI_PASSWORD=$3
IS_ESP=$4

#Constants
SLEEP_SEC=10
WHITELIST="mapred.reduce.tasks|hive.exec.max.dynamic.partitions.pernode|mapreduce.task.timeout|hive.load.dynamic.partitions.thread|hive.stats.autogather|hive.stats.column.autogather|hive.metastore.dml.events"

if [ $IS_ESP = 'Y' ]; then
  sudo su hive
fi

echo "Create Directories"

if [ -d "repos" ]; then
  echo "Directory repos exists."
else
  mkdir repos
fi

cd repos

if [ -d "tpcds-hdinsight" ]; then
  echo "Directory tpcds-hdinsight exists."
  rm -r tpcds-hdinsight
fi

echo "Clone tpcds-hdinsight"
git clone https://github.com/mabushaireh/tpcds-hdinsight.git

cd tpcds-hdinsight

if [ $IS_ESP = 'Y' ]; then
  exit
fi

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

  echo "Copy resources files to hdf tmp folder!"
  sudo su hive
  hdfs dfs -copyFromLocal resources /tmp

  echo "Generate Data!"
  /usr/bin/hive -i settings.hql -f TPCDSDataGen.hql -hiveconf SCALE=2 -hiveconf PARTS=10 -hiveconf LOCATION=/HiveTPCDS/ -hiveconf TPCHBIN=$(grep -A 1 "fs.defaultFS" /etc/hadoop/conf/core-site.xml | grep -o "wasb[^<]*")/tmp/resources
  echo "Create External Tables!"
  /usr/bin/hive -i settings.hql -f ddl/createAllExternalTables.hql -hiveconf LOCATION=/HiveTPCDS/ -hiveconf DBNAME=tpcds
  echo "Create ORC Tables!"
  /usr/bin/hive -i settings.hql -f ddl/createAllORCTables.hql -hiveconf ORCDBNAME=tpcds_orc -hiveconf SOURCE=tpcds
  echo "Analyze Tables!"
  /usr/bin/hive -i settings.hql -f ddl/analyze.hql -hiveconf ORCDBNAME=tpcds_orc

  echo "Run Queries Tables!"
  for f in queries/*.sql; do for i in {1..1}; do
    STARTTIME="$(date +%s)"
    /usr/bin/hive -i settings.hql -f $f -hiveconf ORCDBNAME=tpcds_orc >$f.run_$i.out 2>&1
    SUCCESS=$?
    ENDTIME="$(date +%s)"
    echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>times_orc.csv
  done; done
else
  echo "Copy resources files to hdf tmp folder!"

  hdfs dfs -copyFromLocal resources /tmp

  echo "Generate Data!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f TPCDSDataGen.hql -hiveconf SCALE=2 -hiveconf PARTS=10 -hiveconf LOCATION=/HiveTPCDS/ -hiveconf TPCHBIN=$(grep -A 1 "fs.defaultFS" /etc/hadoop/conf/core-site.xml | grep -o "wasb[^<]*")/tmp/resources
  echo "Create External Tables!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f ddl/createAllExternalTables.hql -hiveconf LOCATION=/HiveTPCDS/ -hiveconf DBNAME=tpcds
  echo "Create ORC Tables!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f ddl/createAllORCTables.hql -hiveconf ORCDBNAME=tpcds_orc -hiveconf SOURCE=tpcds
  echo "Analyze Tables!"
  /usr/bin/hive -n "" -p "" -i settings.hql -f ddl/analyze.hql -hiveconf ORCDBNAME=tpcds_orc

  echo "Run Queries Tables!"
  for f in queries/*.sql; do for i in {1..1}; do
    STARTTIME="$(date +%s)"
    /usr/bin/hive -i settings.hql -f $f -hiveconf ORCDBNAME=tpcds_orc >$f.run_$i.out 2>&1
    SUCCESS=$?
    ENDTIME="$(date +%s)"
    echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>times_orc.csv
  done; done
fi
