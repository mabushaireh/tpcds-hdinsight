set hive.exec.max.dynamic.partitions.pernode=5000;
set mapreduce.task.timeout=360000000;
set hive.load.dynamic.partitions.thread=100;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;
set hive.metastore.dml.events=false;
set hive.tez.java.opts=-Xmx3276m;
set hive.tez.container.size=4096;