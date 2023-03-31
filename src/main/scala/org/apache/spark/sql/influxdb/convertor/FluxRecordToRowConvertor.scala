package org.apache.spark.sql.influxdb.convertor

import com.influxdb.query.FluxRecord
import org.apache.spark.sql.influxdb.util.Nanos
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object FluxRecordToRowConvertor {

  def rawDataSchema = StructType(StructField("value", StringType) :: Nil)

  def rawDataToLineProtocol(record: FluxRecord) = {
    val timestamp = Nanos.toNano(record.getTime)
    val measurement = record.getMeasurement
    val fieldSet = Array(s"${record.getField}=${record.getValue}").mkString(",")
    val tagSet: Option[String] = record.getValues.asScala.size match {
      case _@s if s > 8 =>
        val entry = record.getValues.asScala.toArray
        val ts = (9 to entry.length).map { idx =>
          val (tk, tv) = entry(idx - 1)
          s"${tk}=${tv}"
        }.toArray.mkString(",")
        Some(ts)
      case _ =>
        None
    }
    tagSet match {
      case Some(_tagSet) =>
        s"${measurement},${_tagSet} ${fieldSet} ${timestamp}"
      case _ =>
        s"${measurement} ${fieldSet} ${timestamp}"
    }
  }
}
