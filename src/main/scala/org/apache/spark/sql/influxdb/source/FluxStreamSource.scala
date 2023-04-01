package org.apache.spark.sql.influxdb.source

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, OffsetSeqLog, SerializedOffset, Source}
import org.apache.spark.sql.influxdb.config.FluxOptions
import org.apache.spark.sql.influxdb.convertor.FluxRecordToRowConvertor
import org.apache.spark.sql.influxdb.source.FluxStreamSource._
import org.apache.spark.sql.influxdb.util.{FluxClient, Nanos, TimeSpliter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.influxdb.offset.LongOffset
import org.apache.spark.sql.influxdb.rdd.FluxSourceRDD
import org.apache.spark.unsafe.types.UTF8String

import java.util.concurrent.locks.{Lock, ReentrantLock}

class FluxStreamSource(
  sqlContext: SQLContext,
  metadataPath: String,
  schema: Option[StructType],
  providerName: String,
  parameters: Map[String, String]
) extends Source with Logging {

  private var lastOffset: Long = -1L

  private var client: FluxClient = _

  private val lock: Lock = new ReentrantLock()

  initialize

  /**
   * Lazily init latest offset from checkpoint
   */
  private lazy val recoveredLatestOffsetFromCheckpoint = {
    log.info(
      """
        |------------------------------------------
        |Recover from checkpoint
        |------------------------------------------
        |""".stripMargin)

    val currentSourceIndex = {
      if (!metadataPath.isEmpty) {
        metadataPath.substring(metadataPath.lastIndexOf("/") + 1).toInt
      } else {
        -1
      }
    }
    if (currentSourceIndex >= 0) {
      val rootCheckpointPath = new Path(metadataPath).getParent.getParent.toUri.toString
      logInfo(
        s"""
          |Checkpoint path: ${rootCheckpointPath}
          |------------------------------------------
          |""".stripMargin)
      val offsetLog = new OffsetSeqLog(sqlContext.sparkSession,
        new Path(rootCheckpointPath, CHECKPOINT_OFFSETS).toUri.toString)
      // get the latest offset from checkpoint
      offsetLog.getLatest() match {
        case Some((batchId, offset)) =>
          logInfo(
            s"""
              |Checkpoint data:
              |  [batchId]: ${batchId}
              |  [offset] : ${offset}
              |------------------------------------------
              |""".stripMargin)
          offset.offsets.toArray match {
            case Array(Some(_@_offset), _*) =>
              Some(getOffsetValue(_offset.asInstanceOf[Offset]))
            case _ =>
              None
          }
        case None =>
          logInfo(
            """
              |Checkpoint data: None
              |------------------------------------------
              |""".stripMargin)
          None
      }
    } else {
      throw new IllegalStateException("You must be set `checkpointLocation`!!! ")
      None
    }
  }

  private def initialize(): Unit = {
    // Initialize Flux query client
    client = FluxClient(FluxOptions.createInfluxDBClientOptions(parameters))

    // Recover lastOffset from checkpoint
    withLock[Unit] {
      recoveredLatestOffsetFromCheckpoint match {
        case Some(_offset) =>
          lastOffset = _offset
          logInfo(s"Recover last offset value ${lastOffset}")
        case _ =>
          logInfo(s"Not find recoverable last offset")
      }
    }
  }

  private def hasArrivalRowForCurrentBatch = client.maximumTime(
    FluxOptions.bucket(parameters), FluxOptions.measurement(parameters)) match {
      case Some(_maxOffset) =>
        Nanos.toNano(_maxOffset) > lastOffset
      case None =>
        false
  }

  private def withLock[T](body: => T): T = {
    lock.lock()
    try body
    finally lock.unlock()
  }

  override def schema: StructType = FluxRecordToRowConvertor.rawDataSchema

  override def getOffset: Option[Offset] = {
    withLock[Option[Offset]] {
      val _offset = lastOffset match {
        // First submit and get min _time for influxdb
        case -1 =>
          client.minimumTime(FluxOptions.bucket(parameters), FluxOptions.measurement(parameters)) match {
            case Some(_minOffset) =>
              lastOffset = Nanos.toNano(_minOffset)
              Some(LongOffset(Nanos.toNano(_minOffset)))
            case _ =>
              None
          }
        case _ => hasArrivalRowForCurrentBatch match {
          // Row arrival
          case true =>
            lastOffset = Nanos.toNano(Nanos.add(Nanos.parse(lastOffset), FluxOptions.deltaTime(parameters)))
            Some(LongOffset(lastOffset))
          // No message has arrived for this batch.
          case _ =>
            Some(LongOffset(lastOffset))
        }
      }
      logInfo(
        s"""
          |------------------------------------------
          |Get Offset
          |------------------------------------------
          |lastOffset : ${_offset}(${_offset.map(off => Nanos.parse(off.offset))})
          |------------------------------------------
          |""".stripMargin)
      _offset
    }
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    logInfo(
      s"""
        |------------------------------------------
        |Get Batch
        |------------------------------------------
        |Called with start = ${start}, end = ${end}
        |------------------------------------------
        |lastOffset: ${lastOffset}
        |------------------------------------------
        |""".stripMargin)

    // This situation indicates that the current batch is being consumed for the first time.
    // At this point, the end offset is the minimum timestamp in the influxdb measurement,
    // So, do not process at this time.
    if (start.isEmpty) {
      logInfo(s"The start offset of the batch is none")
      return emptyRDD(sqlContext)
    }

    // This indicates that it is not the first consumption, and the initial offset is equal, without processing.
    if (start.isDefined && start.get == end) {
      logInfo(s"The batch data is None!")
      return emptyRDD(sqlContext)
    }

    // to do
    val startOffset = getOffsetValue(start.get)
    val endOffset = getOffsetValue(end)
    val startInstant = Nanos.parse(startOffset)
    val endInstant = Nanos.parse(endOffset)

    logInfo(
      s"""
        |------------------------------------------------------------------------------------
        | Offset range
        |------------------------------------------------------------------------------------
        | Raw offset:     [${startOffset}, ${endOffset}]
        |------------------------------------------------------------------------------------
        | Instant offset: [${startInstant}, ${endInstant}]
        |------------------------------------------------------------------------------------
        |""".stripMargin)

    val sc = sqlContext.sparkContext
    val numPartitions = sc.defaultParallelism
    val slices = TimeSpliter.slice(startInstant, endInstant, numPartitions)

    val rdd = new FluxSourceRDD(sc, slices, parameters)

    sqlContext.internalCreateDataFrame(
      rdd.setName("influxdb"), FluxRecordToRowConvertor.rawDataSchema, isStreaming = true)
  }

  override def stop(): Unit = {
    logInfo("Stop influxdb source.")
    client.onStop
  }
}

object FluxStreamSource {

  private val CHECKPOINT_OFFSETS = "offsets"

  private def getOffsetValue(offset: Offset): Long = offset match {
    case o: org.apache.spark.sql.influxdb.offset.LongOffset =>
      o.offset
    case so: SerializedOffset =>
      so.json.toLong
  }

  private def emptyRDD(sqlContext: SQLContext) = sqlContext.internalCreateDataFrame(
    sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
    FluxRecordToRowConvertor.rawDataSchema,
    isStreaming = true)
}
