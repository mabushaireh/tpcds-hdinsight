SET mapred.reduce.tasks=${hivevar:PARTS} ;
set hive.query.name=${hivevar:QUERY}_TPCDSgen;

ADD FILE ${hivevar:TPCHBIN}/dsdgen;
ADD FILE ${hivevar:TPCHBIN}/tpcds.idx;
ADD FILE ${hivevar:TPCHBIN}/sequenceGenerator.py;
ADD FILE ${hivevar:TPCHBIN}/TPCDSgen.py;

FROM (
    SELECT TRANSFORM(x) 
    USING 'python sequenceGenerator.py "${hivevar:SCALE}"' AS (key INT, value STRING) 
    FROM ( SELECT 1 x) t 
    DISTRIBUTE BY (hash(key) % "${hivevar:PARTS}")
    ) d REDUCE d.key 
USING 'python TPCDSgen.py -s "${hivevar:SCALE}" -o "${hivevar:LOCATION}" -n "${hivevar:PARTS}"' ;
