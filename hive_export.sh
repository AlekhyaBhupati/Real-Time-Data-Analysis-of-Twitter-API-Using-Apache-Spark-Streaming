hive --hiveconf hive.server2.logging.operation.level=NONE -e "set hive.server2.logging.operation.level=NONE; select * from report_max_follow_by_location;" | sed '/Fetched:\|parquet.hadoop.ParquetRecordReader:\|WARN:\|INFO:/,+1 d' |  sed 's/[[:space:]]\+/,/g' > ../reports/report_max_follow_by_location.csv

hive --hiveconf hive.server2.logging.operation.level=NONE -e "set hive.server2.logging.operation.level=NONE; select * from report_user_by_location;" | sed '/Fetched:\|parquet.hadoop.ParquetRecordReader:\|WARN:\|INFO:/,+1 d' |  sed 's/[[:space:]]\+/,/g' > ../reports/report_user_by_location.csv

