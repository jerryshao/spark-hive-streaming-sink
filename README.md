Spark Hive Streaming Sink
===

A sink to save Spark Structured Streaming DataFrame into Hive table


This sink:

1. Saves Structured Streaming micro-batch/continuous-processing(Spark 2.3+) DataFrame into hive table.
2. Uses isolated classloader to isolate Hive related dependencies, which means it can support different versions of Hive other than Spark's built-in one.
3. Uses newest DataSource API V2, which means it can only be worked with Spark 2.3+.

The details of Hive Streaming could be referred [here](https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest).

How To Build
==========

To use this connector, you will require a latest version of Spark (Spark 2.3+).

To build this project, please execute:

```shell
mvn package -DskipTests
```

`mvn package` will generate two jars，including one uber jar. User could use this uber jar at convenience.

How To Use
==========

1. This Spark hive streaming sink jar should be loaded into Spark's environment by `--jars`.
2. A required Hive table should be created before ingesting data into this table. The requirement can be checked [here](https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest#StreamingDataIngest-StreamingRequirements).
3. A `hive-site.xml` with required configurations should be put into Spark classpath, so that it can be accessed from classloader.
4. If you're running in a secured environment, then principal and keytab should be provided.

Please be aware a valid `hive-site.xml` and keytab should be accessible from executor side, which means user should pass it via `--files`.

To use this library, it is similar to other data source libraries, for example:

```scala
val socket = sparkSession.readStream
  .format("socket")
  .options(Map("host" -> host, "port" -> port))
  .load()
  .as[String]

val query = socket.map { s =>
  val records = s.split(",")
  assert(records.length >= 4)
  (records(0).toInt, records(1), records(2), records(3))
}
  .selectExpr("_1 as id", "_2 as msg", "_3 as continent", "_4 as country")
  .writeStream
  .format("hive-streaming")
  .option("metastore", metastoreUri)
  .option("db", "default")
  .option("table", "alerts")
  .queryName("socket-hive-streaming")
  .start()
```

User should convert the data source schema to match the destination table's schema, like above `.selectExpr("_1 as id", "_2 as msg", "_3 as continent", "_4 as country")`.

User should specify the data source format `hive-streaming` and required options:

1. `metastore`, metastore uris for which to connect to.
2. `db`, db name to write to.
3. `table`, table name to write to.

Above 3 options are required to run hive streaming application, for others please check here:

option | default value | meaning
------ | ------------- | -------
txn.per.batch | 100    | Hive grants a batch of transactions instead of single transactions to streaming clients.This setting configures the number of desired transactions per Transaction Batch. Data from all transactions in a single batch end up in a single file. Flume will write a maximum of batchSize events in each transaction in the batch. This setting in conjunction with batch.size provides control over the size of each file. Note that eventually Hive will transparently compact these files into larger files.
auto.create.partitions |  true  | automatically create the necessary Hive partitions to stream to.
principal  | none  | Kerberos user principal for accessing secure Hive.
keytab  | none  | Kerberos keytab for accessing secure Hive.
batch.size  | 10000  |  Max number of events written to Hive in a single Hive transaction.

License
=======

Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0.
