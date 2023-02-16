set hive.tez.java.opts=-Xmx6800m; -- 0.8 * hive.tez.container.size
set hive.tez.container.size=8000;
set tez.runtime.io.sort.mb=3200; -- 0.4 * hive.tez.container.size
set tez.runtime.unordered.output.buffer.size-mb=320; 
set tez.grouping.max-size=33554432;
