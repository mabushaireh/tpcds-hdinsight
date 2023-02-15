set hive.exec.max.dynamic.partitions.pernode=1000;
set mapreduce.task.timeout=360000000;
set hive.load.dynamic.partitions.thread=50;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;
set hive.metastore.dml.events=false;
set hive.tez.java.opts=-Xmx6800m; -- 0.8 * hive.tez.container.size
set hive.tez.container.size=8000;
set tez.runtime.io.sort.mb=3200; -- 0.4 * hive.tez.container.size
set tez.runtime.unordered.output.buffer.size-mb=320; --0.1 * hive.tez.container.size
set hive.map.aggr=false;

set hive.auto.convert.join=false;
set hive.auto.convert.join.noconditionaltask=false;
set hive.query.results.cache.enabled=false;
set hive.compute.query.using.stats=false;
set hive.fetch.task.conversion=none;