#!/bin/bash

#Variables
CLUSTER_NAME="mas433-nonesp-wasb-707"
AMBARI_USER=$1

echo $1

#Constants
SLEEP_SEC=10
WHITELIST="hive.exec.max.dynamic.partitions.pernode|mapreduce.task.timeout|hive.load.dynamic.partitions.thread|hive.stats.autogather|hive.stats.column.autogather|hive.metastore.dml.events"

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
      echo "Staring"
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

echo "Generate tpds data, fromat is rc and size is 2 GB"
/usr/bin/hive -i settings.hql -f TPCDSDataGen.hql -hiveconf SCALE=10 -hiveconf PARTS=10 -hiveconf LOCATION=/HiveTPCDS/ -hiveconf TPCHBIN=resources

/usr/bin/hive -i settings.hql -f ddl/createAllExternalTables.hql -hiveconf LOCATION=/HiveTPCDS/ -hiveconf DBNAME=tpcds

