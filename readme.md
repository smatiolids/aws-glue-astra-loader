# AWS Glue/Pyspark for loading data into Astra

Environment:

- Spark 3 or 4
- Dataframes (not RDD)
- Source data stored on S3.

Started with the [Awesome-Astra example for Glue](https://awesome-astra.github.io/docs/pages/data/explore/awsglue/). However, it does provide a model for loading data into Astra DB, but the other way around.

Anyway, I leveraged [the security setup and secret management from the Awesome-Astra page](https://awesome-astra.github.io/docs/pages/data/explore/awsglue/#step-14-secrets-manager). I just added [Astra DB's `token`](https://docs.datastax.com/en/astra/astra-db-vector/administration/manage-application-tokens.html) to the secrets manager.


## Python Script

The script is simple. Main points to note:

- Handle the secret manager connection for accessing the Astra DB's `token`.
- Format specification.
- Loading the S3 source file (CSV format).

## Job Details

Here, we can set the versions to be used—attention to the Spark version.



### Files
For this case, the script needs two files to be stored in a S3 Bucket:

- `spark-cassandra-connector-assembly_2.12-3.3.0.jar`
  - Downloaded from: https://search.maven.org/artifact/com.datastax.spark/spark-cassandra-connector-assembly_2.12/3.3.0/jar
- The Spark & Scala versions used in [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html) must match the [DataStax Spark Cassandra Connector's version](https://github.com/datastax/spark-cassandra-connector?tab=readme-ov-file#version-compatibility).
  - It needs to use the assembly version, which has all the dependencies included in it.

- Secure connection bundle
  - Generated on Astra dashboard and uploaded into S3

These files should be referenced in the "Job Details" page:

### Job parameters
```
–conf
spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions
```

and

```
--packages
com.datastax.spark:spark-cassandra-connector_2.12:3.5.0
```

It is possible to add the Astra credentials to the –conf parameter if needed.

### Execution

Monitor the execution on "Runs" screen.

---
