set hive.merge.cardinality.check=true;

set tez.runtime.compress=true;

set tez.runtime.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

set hive.convert.join.bucket.mapjoin.tez=false;

set hive.exec.reducers.bytes.per.reducer=15728640;

set hive.optimize.sort.dynamic.partition=false;

set hive.tez.max.partition.factor=3f;

set hive.tez.min.partition.factor=1f;

set hive.stats.autogather=false;

set hive.stats.column.autogather=false;