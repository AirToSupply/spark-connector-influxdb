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

SQL Stream can be created with data streams received through TDengine using:

SQL Stream may be also transferred into TDengine using:

## Configuration


| Parameter Name | Description                         | Default Value | Read | Write |
| -------------- | ----------------------------------- | ------------- | ---- | ----- |
| host           | 【Require】InfluxDB Server host     | localhost     | ✅   | ✅    |
| port           | 【Require】InfluxDB Server post     | 8086          | ✅   | ✅    |
| user           | 【Require】InfluxDB Server user     |               | ✅   | ✅    |
| password       | 【Require】InfluxDB Server password |               | ✅   | ✅    |
| org            | 【Require】InfluxDB organization    |               | ✅   | ✅    |
| bucket         | 【Require】InfluxDB bucket          |               | ✅   | ✅    |
| measurement    | 【Require】InfluxDB measurement     |               | ✅   | ✅    |
