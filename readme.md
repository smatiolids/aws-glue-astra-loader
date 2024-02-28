# AWS Glue/Pyspark for loading data into Astra

Environment:

- Spark 3 or 4
- Dataframes (not RDD)
- Source data stored on S3.

Started with the Awesome-Astra example for Glue (https://awesome-astra.github.io/docs/pages/data/explore/awsglue/). However, it doesn't provide a model for loading data into Astra.

Anyway, I leveraged the security setup and secret management from the Awesome-Astra page. I just added to the secrets the astra client ID and secret:


## Python Script

The script is simple. Main points to note:

- Handle the secret manager connection for accessing the astra client ID and secret
- Format specification
- Loading the S3 source file (CSV format).

## Job Details

Here, we can set the versions to be used—attention to the Spark version.



### Files
For this case, the script needs two files to be stored in a S3 Bucket:

Spark-cassandra-connector-assembly_2.12-3.3.0.jar
Downloaded from: https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector-assembly_2.12/3.3.0/jar
The spark version used in Glue must match the connector's version.
It needs to use the assembly version, which has all the dependencies included in it.

Secure connection bundle
Generated on Astra dashboard and uploaded into S3

These files should be referenced in the "Job Details" page:


### Job parameters

–conf
spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions

--packages
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0


It is possible to add the Astra credentials to the –conf parameter if needed.

### Execution

Monitor the execution on "Runs" screen
