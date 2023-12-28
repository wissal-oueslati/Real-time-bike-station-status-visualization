spark-submit \
  --master local[*]\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-hive_2.12:3.2.4 \
  --conf "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,org.apache.spark:spark-hive_2.12:3.2.4" \
  hive_store.py > out.txt
