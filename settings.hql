set hive.tez.java.opts=-Xmx12800m; -- 0.8 * hive.tez.container.size
set hive.tez.container.size=16000;
set tez.runtime.io.sort.mb=6400; -- 0.4 * hive.tez.container.size
set tez.runtime.unordered.output.buffer.size-mb=1600;  -- 0.1 of hive.tex.container.size

set tez.grouping.max-size=2097152; -- default is 1GB I reduced it to increase mappers and reduce the size of the split
set tez.grouping.min-size=2097; -- reduce it for the same same reason above
set hive.exec.max.dynamic.partitions.pernode=600; -- so query finish faster
set mapreduce.task.timeout=360000000; -- so query finish faster
set hive.load.dynamic.partitions.thread=10; -- so query finish faster
set hive.stats.autogather=false; -- so query finish faster
set hive.stats.column.autogather=false; -- so query finish faster
set hive.metastore.dml.events=false; -- so query finish faster