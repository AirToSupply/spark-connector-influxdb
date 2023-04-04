package org.apache.spark.sql.influxdb.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.influxdb.sink.FluxStreamSink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

class FluxSinkProvider extends DataSourceRegister
  with StreamSinkProvider
  with Logging {

  override def shortName(): String = "influxdb"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = new FluxStreamSink(sqlContext, parameters, outputMode)
}
