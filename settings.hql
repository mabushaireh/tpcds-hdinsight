set hive.tez.java.opts=-Xmx6800m; -- 0.8 * hive.tez.container.size
set hive.tez.container.size=8000;
set tez.runtime.io.sort.mb=3200; -- 0.4 * hive.tez.container.size
set tez.runtime.unordered.output.buffer.size-mb=320; 
set tez.grouping.max-size=2097152;
set tez.grouping.min-size=2097;
set hive.exec.max.dynamic.partitions.pernode=5000;
set mapreduce.task.timeout=360000000;
set hive.load.dynamic.partitions.thread=100;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;
set hive.metastore.dml.events=false;