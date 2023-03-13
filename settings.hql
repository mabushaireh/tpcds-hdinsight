set hive.optimize.index.filter=false;
set hive.exec.orc.split.strategy=BI;
set hive.tez.container.size=82768;
set hive.tez.java.opts=-Xmx86214m;
set tez.runtime.io.sort.mb=26192;
set tez.task.resource.memory.mb=82768;
set tez.am.resource.memory.mb=82768;
set tez.am.launch.cmd-opts=-Xmx66214m;
set hive.auto.convert.join=false;
set hive.exec.max.dynamic.partitions.pernode=5000;
set mapreduce.task.timeout=360000000;
set hive.load.dynamic.partitions.thread=100;
set hive.stats.autogather=false;
set hive.stats.column.autogather=false;
set hive.metastore.dml.events=false;
set hive.cbo.enable=true;
set hive.vectorized.execution.enabled=true; 
set dfs.datanode.max.xcievers=4096;