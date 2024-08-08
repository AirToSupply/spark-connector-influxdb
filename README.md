# Spark Connector InfluxDB Data Source

A library for writing and reading data from InfluxDB using Spark SQL Streaming (or Structured streaming).

## Linking

Install package By Maven

```shell
mvn clean install -Dmaven.test.skip=true
```

If you need to deploy private Nexus Repository:

```shell
mvn clean deploy -Dmaven.test.skip=true
```

Import POM to your project:

```shell
<dependency>
    <groupId>tech.odes</groupId>
    <artifactId>spark-connector-influxdb</artifactId>
    <version>{{site.SPARK_VERSION}}</version>
</dependency>
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.

The `--packages` argument can also be used with `bin/spark-submit`.

This library is only for Scala 2.12.x, so users should replace the proper Scala version in the commands listed above.

## Examples

### Read

SQL Stream can be created with data streams received through InfluxDB using:

```scala
spark.readStream
  .format(classOf[FluxSourceProvider].getName)
  .option("host", "localhost")
  .option("port", "8086")
  .option("user", "influxdb")
  .option("password", "influxdb")
  .option("org", "org")
  .option("bucket", "test_bucket")
  .option("measurement", "sensor")
  .load()
```

You can refer to [FluxStreamSourceApplication.scala](examples%2Fsrc%2Fmain%2Fscala%2Forg%2Fapache%2Fspark%2Fsql%2Fexample%2Fsource%2FFluxStreamSourceApplication.scala).

【TIPS】

（1）The source uses the `Minimum Time Slice Algorithm` to extract data from InfluxDB through `incremental reads`.

（2）The read data currently only supports raw record based on the StringType string situation of `Line Protocol`.

### Write

SQL Stream may be also transferred into InfluxDB using:

```scala
spark.writeStream
  .format(classOf[FluxSinkProvider].getName)
  .outputMode(OutputMode.Append())
  .option("host", "localhost")
  .option("port", "8086")
  .option("user", "influxdb")
  .option("password", "influxdb")
  .option("org", "org")
  .option("bucket", "test_bucket")
  .option("checkpointLocation", "/tmp")
  .start()
  .awaitTermination()
```

You can refer to [FluxStreamSinkApplication.scala](examples%2Fsrc%2Fmain%2Fscala%2Forg%2Fapache%2Fspark%2Fsql%2Fexample%2Fsink%2FFluxStreamSinkApplication.scala).

【TIPS】

（1）The Sink only supports data sources based on `line protocols`.

（2）`OutputMod`e mode only supports `append` mode.

## Configuration


| Parameter Name | Description                                                     | Default Value | Read | Write |
|----------------|-----------------------------------------------------------------| ------------- | ---- | ----- |
| host           | 【Require】InfluxDB Server host                                   | localhost     | ✅   | ✅    |
| port           | 【Require】InfluxDB Server post                                   | 8086          | ✅   | ✅    |
| user           | 【Require】InfluxDB Server user                                   |               | ✅   | ✅    |
| password       | 【Require】InfluxDB Server password                               |               | ✅   | ✅    |
| token          | InfluxDB Server access token                                    |               | ✅   | ✅    |
| org            | 【Require】InfluxDB organization                                  |               | ✅   | ✅    |
| bucket         | 【Require】InfluxDB bucket                                        |               | ✅   | ✅    |
| measurement    | 【Require】InfluxDB measurement                                   |               | ✅   |       |
| delta-time     | Incremental read from influxdb by minimum time slice (Unit: ms) | 1000          | ✅   |       |
| time-zone      | A time-zone ID, such as Europe/Paris.                           | Asia/Shanghai | ✅   |       |
| batchSize      | Specify how many pieces of data to write as a batch.            | 1000          |      | ✅    |
| numPartitions  | Number of partitions to write.                                  | None          |      | ✅    |
