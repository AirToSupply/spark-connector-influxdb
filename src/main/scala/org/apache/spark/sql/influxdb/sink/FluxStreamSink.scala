package org.apache.spark.sql.influxdb.sink

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.influxdb.write.LineProtocolWrite
import org.apache.spark.sql.streaming.OutputMode

class FluxStreamSink(
  sqlContext: SQLContext,
  parameters: Map[String, String],
  outputMode: OutputMode) extends Sink with Logging {

  @volatile private var latestBatchId = -1L

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      log.info(s"Skipping already committed batch $batchId")
    } else {
      outputMode match {
        case _ if outputMode == OutputMode.Append =>
          LineProtocolWrite.write(sqlContext.sparkSession, data, parameters)
        case _@mode =>
          throw new IllegalArgumentException(s"Data source does not support $mode output mode")
      }
      latestBatchId = batchId
    }
  }
}
