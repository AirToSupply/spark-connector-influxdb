package org.apache.spark.sql.influxdb.write

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.influxdb.config.FluxOptions
import org.apache.spark.sql.influxdb.util.FluxClient

class LineProtocolWriteTask(
  partitionId: Int,
  options: Map[String, String],
  batchSize: Int) extends Logging {

  def execute(iterator: Iterator[InternalRow]): Unit = {
    logInfo(s"LineRecordWriteTask execute by partition (${partitionId})")

    val client = FluxClient(FluxOptions.createInfluxDBClientOptions(options))

    try {
      var rowCount = 0
      val records = scala.collection.mutable.ArrayBuffer[String]()
      while (iterator.hasNext) {
        val row = iterator.next()
        records.append(row.getString(0))
        rowCount += 1
        if (rowCount % batchSize == 0) {
          client.writeRecordIterator(FluxOptions.bucket(options), records.toIterator)
          rowCount = 0
          records.clear()
        }
      }
      if (records.nonEmpty) {
        client.writeRecordIterator(FluxOptions.bucket(options), records.toIterator)
        rowCount = 0
        records.clear()
      }
    } catch {
      case e: Exception =>
        logError(s"Writing data failed for partition [${partitionId}], cause by", e)
    } finally {
      try {
        if (null != client) {
          client.onStop
        }
      } catch {
        case e: Exception =>
          logWarning(s"Writing data succeeded, but closing failed for partition [${partitionId}]", e)
      }
    }

  }

}

object LineProtocolWriteTask {
  def apply(
    partitionId: Int,
    options: Map[String, String],
    batchSize: Int) = new LineProtocolWriteTask(partitionId, options, batchSize)
}
