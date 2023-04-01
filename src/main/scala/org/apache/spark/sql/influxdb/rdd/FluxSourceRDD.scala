package org.apache.spark.sql.influxdb.rdd

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.influxdb.config.FluxOptions
import org.apache.spark.sql.influxdb.convertor.FluxRecordToRowConvertor
import org.apache.spark.sql.influxdb.util.{FluxClient, TimeSlice}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.CompletionIterator

import java.time.Instant

case class FluxSourceRDDPartition(index: Int, start: Instant, end: Instant) extends Partition {
  override def toString: String =
    s"""
       |FluxSourceRDDPartition
       |  index         : ${index}
       |  start         : ${start}
       |  end           : ${end}
       |""".stripMargin
}

class FluxSourceRDD(
  sc: SparkContext,
  slices: Array[TimeSlice],
  options: Map[String, String]) extends RDD[InternalRow](sc, Nil) {

  override def persist(level: StorageLevel): this.type = super.persist(level)

  override def compute(partition: Partition, context: TaskContext): Iterator[InternalRow] = {
    val client = FluxClient(FluxOptions.createInfluxDBClientOptions(options))
    var closed = false
    val part = partition.asInstanceOf[FluxSourceRDDPartition]

    def close(): Unit = {
      if (closed) return
      try {
        if (null != client) {
          client.onStop
        }
      } catch {
        case e: Exception => logWarning("Exception closing FluxClient", e)
      }
      closed = true
    }

    context.addTaskCompletionListener[Unit]{ context => close() }

    logInfo(
      s"""
         |------------------------------------------
         | Compute:
         |------------------------------------------
         | ${part}
         |------------------------------------------
         |""".stripMargin)

    val rowsIterator = client.queryByTimeRange(
      FluxOptions.bucket(options), FluxOptions.measurement(options), part.start, part.end)
      .map { record =>
        InternalRow(
          UTF8String.fromString(FluxRecordToRowConvertor.rawDataToLineProtocol(record))
        )
      }

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())
  }

  override protected def getPartitions: Array[Partition] = slices.zipWithIndex.map {
    case (slice, index) =>
      FluxSourceRDDPartition(index, slice.start, slice.stop)
  }.toArray
}
