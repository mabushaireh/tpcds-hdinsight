#!/bin/bash
echo $@
SLEEP_SEC=10
QUERY_COUNT=1
WHITELIST="mapred.reduce.tasks|hive.exec.max.dynamic.partitions.pernode|mapreduce.task.timeout|hive.load.dynamic.partitions.thread|hive.stats.autogather|hive.stats.column.autogather|hive.metastore.dml.events|hive.tez.java.opts|hive.tez.container.size|tez.runtime.io.sort.mb|tez.runtime.unordered.output.buffer.size-mb|tez.grouping.max-size|tez.grouping.min-size|hive.query.name"
FORMAt=None
LIMIT=100
SKIP=0
SCALE=1

#Params
while getopts ":f:c:h:u:p:s:q:g:l:k:" opt; do
  case ${opt} in
  f)
    echo "$OPTARG"
    FORMAT=$OPTARG
    ;;
  c)
    CLEANUP=$OPTARG
    ;;
  h)
    CLUSTER_NAME=$OPTARG
    ;;
  u)
    AMBARI_USER=$OPTARG
    ;;
  p)
    AMBARI_PASSWORD=$OPTARG
    ;;
  s)
    IS_ESP=$OPTARG
    ;;
  q)
    EXECUTE_QUERY=$OPTARG
    ;;
  g)
    GENERATE_TABLES=$OPTARG
    ;;
  l)
    LIMIT=$OPTARG
    ;;
  k)
    SKIP=$OPTARG
    ;;
  \?)
    echo "Invalid option: -$OPTARG" 1>&2
    exit 1
    ;;
  :)
    echo "Option -$OPTARG requires an argument." 1>&2
    exit 1
    ;;
  esac
done

echo "FORMAT is set to $FORMAT"
echo "CLUSTER_NAME is set to $CLUSTER_NAME"
echo "AMBARI_USER is set to $AMBARI_USER"
echo "AMBARI_PASSWORD is set to ****"
echo "IS_ESP is set to $IS_ESP"
echo "EXECUTE_QUERY is set to $EXECUTE_QUERY"
echo "GENERATE_TABLES is set to $GENERATE_TABLES"
echo "LIMIT is set to $LIMIT"
echo "SKIP is set to $SKIP"

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
  sudo /var/lib/ambari-server/resources/scripts/configs.py -p 8080 -a set -l headnodehost -c hive-site -n $CLUSTER_NAME -k "parquet.memory.min.chunk.size" -v "524288" -u $AMBARI_USER -p "$AMBARI_PASSWORD"
  
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
  echo "IS_ESP already applied! $IS_ESP"

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
  /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -f TPCDSDataGen.hql -hivevar SCALE=3 -hivevar PARTS=10 -hivevar LOCATION=/HiveTPCDS/ -hivevar TPCHBIN=$(grep -A 1 "fs.defaultFS" /etc/hadoop/conf/core-site.xml | grep -o "abfs[^<]*")/tmp/resources --hivevar QUERY=TPCDSDataGen_$(date '+%Y%m%d_%H%M%S')
  echo "Create External Tables!"
  /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -f ddl/createAllExternalTables.hql -hivevar LOCATION=/HiveTPCDS/ -hivevar DBNAME=tpcds --hivevar QUERY=createAllExternalTables_$(date '+%Y%m%d_%H%M%S')
  echo "Analyze External Tables!"
  /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds --hivevar QUERY=analyze_external_$(date '+%Y%m%d_%H%M%S')
fi

if [[ "$FORMAT" == "ALL" || "$FORMAT" == "Parquet" ]]; then
  if [[ "$GENERATE_TABLES" == "Y" ]]; then
    echo "Create Parquet Tables!"
    /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -i settings.hql -f ddl/createAllParquetTables.hql -hivevar PARQUETDBNAME=tpcds_parquet -hivevar SOURCE=tpcds --hivevar QUERY=createAllParquetTables_$(date '+%Y%m%d_%H%M%S')
    echo "Analyze Parquet Tables!"
    /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds_parquet --hivevar QUERY=analyze_Parquet_$(date '+%Y%m%d_%H%M%S')

  fi

  if [[ "$EXECUTE_QUERY" == "Y" ]]; then
    echo "Run Queries Parquet Tables!"
    STAT_FILE=times_Parquet_$(date '+%Y%m%d_%H%M%S').csv

    #stop loop when limit is reached
    count=1
    for f in queries/*.$SCALE.sql; do for i in {1..1}; do
      if [ $count -gt $LIMIT ]; then
        break
      fi
      echo "count is $count"

     ((count++))
      STARTTIME="$(date +%s)"
      echo "Running query $f"
      /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -i settings.hql -f $f -hivevar ORCDBNAME=tpcds_parquet --hivevar QUERY=Parquet_$f.$(date '+%Y%m%d_%H%M%S') >$f.run_$i.out 2>&1
      SUCCESS=$?
      ENDTIME="$(date +%s)"
      echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))"
      echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>$STAT_FILE
    done; done
    Run_FOLDER=Parquet_$(date '+%Y%m%d_%H%M%S')
    hdfs dfs -mkdir /Runs
    hdfs dfs -mkdir /Runs/$Run_FOLDER
    for f in queries/*.out; do
      hdfs dfs -copyFromLocal $f /Runs/$Run_FOLDER
    done
    hdfs dfs -copyFromLocal $STAT_FILE /Runs/$Run_FOLDER

  fi

elif [[ "$FORMAT" == "ALL" || "$FORMAT" == "ORC" ]]; then
  if [[ "$GENERATE_TABLES" == "Y" ]]; then
    echo "Create ORC Tables!"
    /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -i settings.hql -f ddl/createAllORCTables.hql -hivevar ORCDBNAME=tpcds_orc -hivevar SOURCE=tpcds --hivevar QUERY=createAllORCTables_$(date '+%Y%m%d_%H%M%S')
    echo "Analyze ORC Tables!"
    /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -n "" -p "" -f ddl/analyze.hql -hivevar ORCDBNAME=tpcds_orc --hivevar QUERY=analyze_ORC_$(date '+%Y%m%d_%H%M%S')
  fi

  if [[ "$EXECUTE_QUERY" == "Y" ]]; then
    echo "Run Queries ORC Tables!"
    STAT_FILE=times_orc_$(date '+%Y%m%d_%H%M%S').csv
    count=1
    startAt=0
    for f in queries/*.$SCALE.sql; do for i in {1..1}; do
      if [ $startAt -lt $SKIP ]; then
        ((startAt++))
        echo "startAt is $startAt"
        continue
      fi
      if [ $count -gt $LIMIT ]; then
        #if limit is reached, stop loop
        break
      fi
      echo "count is $count"
     ((count++))
      echo "Running query $f"

      STARTTIME="$(date +%s)"
      /usr/bin/hive -u "jdbc:hive2://zk1-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk2-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181,zk5-mas655.veg4amadgz2uvciz5nocsdwf3b.cx.internal.cloudapp.net:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-interactive" -i settings.hql -f $f -hivevar ORCDBNAME=tpcds_orc --hivevar QUERY=ORC_$f.$(date '+%Y%m%d_%H%M%S') >$f.run_$i.out 2>&1
      SUCCESS=$?
      ENDTIME="$(date +%s)"
      echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))"
      echo "$f,$i,$SUCCESS,$STARTTIME,$ENDTIME,$(($ENDTIME - $STARTTIME))" >>$STAT_FILE
    done; done

    Run_FOLDER=ORC_$(date '+%Y%m%d_%H%M%S')
    hdfs dfs -mkdir /Runs
    hdfs dfs -mkdir /Runs/$Run_FOLDER
    for f in queries/*.out; do
      hdfs dfs -copyFromLocal $f /Runs/$Run_FOLDER
    done
    hdfs dfs -copyFromLocal $STAT_FILE /Runs/$Run_FOLDER

  fi

else
  echo "Invalid format"
fi
