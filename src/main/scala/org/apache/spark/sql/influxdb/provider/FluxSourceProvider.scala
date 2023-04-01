package org.apache.spark.sql.influxdb.provider

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.influxdb.convertor.FluxRecordToRowConvertor
import org.apache.spark.sql.influxdb.source.FluxStreamSource
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class FluxSourceProvider extends StreamSourceProvider
  with DataSourceRegister
  with Logging {

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = (shortName, FluxRecordToRowConvertor.rawDataSchema)

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source =
    new FluxStreamSource(sqlContext, metadataPath, schema, providerName, parameters)

  override def shortName(): String = "influxdb"
}
