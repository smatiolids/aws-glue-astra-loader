$SPARK_HOME/bin/spark-submit \
--packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 \
--files <secure bundle> \
--conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions \
astra-glue-loader.py 



