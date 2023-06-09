package org.apache.spark.sql.influxdb.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.domain.WritePrecision
import com.influxdb.client.scala.{InfluxDBClientScala, InfluxDBClientScalaFactory}
import com.influxdb.client.write.Point
import com.influxdb.query.FluxRecord
import com.influxdb.query.dsl.Flux
import com.influxdb.query.dsl.functions.restriction.Restrictions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.influxdb.util.FluxClient._

import java.time.Instant
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class FluxClient(options: InfluxDBClientOptions) extends Logging {

  private implicit var _system: ActorSystem = _

  private var _client: InfluxDBClientScala = _

  onStart

  private def onStart = {
    _system = _createActorSystem
    _client = _createClient(options)
  }

  def onStop = {
    _closeClient(_client)
    _terminateActorSystem(_system)
  }

  private def _createClient(options: InfluxDBClientOptions) =
    InfluxDBClientScalaFactory.create(options)

  private def _closeClient(client: InfluxDBClientScala) = client.close()

  private def _createActorSystem = ActorSystem(ACTOR_SYSTEM_NAME)

  private def _terminateActorSystem(actorSystem: ActorSystem) = actorSystem.terminate()

  def run[T](options: InfluxDBClientOptions)(body: InfluxDBClientScala => T): Option[T] = {
    implicit val system: ActorSystem = ActorSystem(UUID.randomUUID().toString)
    val client = _createClient(options)
    var res = None
    try {
      Some(body(client))
    } catch {
      case e: Exception =>
        client.close()
        None
    } finally {
      client.close()
      system.terminate()
    }
  }

  def minimumTime(bucket: String, measurement: String): Option[Instant] = {
    StopWatch.start
    var _time: Option[Instant] = None
    try {
      val query = Flux.from(bucket)
        .range(RANGE_START)
        .filter(Restrictions.measurement().equal(measurement))
        .first()
      val results = _client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        _time = Some(record.getTime)
      }))
      Await.result(sink, Duration.Inf)
      StopWatch.stop
      logInfo(s"Get minimum _time cost time: ${StopWatch.cost}ms")
      _time
    } catch {
      case e: Exception =>
        logError("Get minimum _time failed!", e)
        _time
    }
  }

  def maximumTime(bucket: String, measurement: String): Option[Instant] = {
    StopWatch.start
    var _time: Option[Instant] = None
    try {
      val query = Flux.from(bucket)
        .range(RANGE_START)
        .filter(Restrictions.measurement().equal(measurement))
        .last()
      val results = _client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        _time = Some(record.getTime)
      }))
      Await.result(sink, Duration.Inf)
      StopWatch.stop
      logInfo(s"Get maximum _time cost time: ${StopWatch.cost}ms")
      _time
    } catch {
      case e: Exception =>
        logError("Get maximum _time failed!", e)
        _time
    }
  }

  def count(bucket: String, measurement: String, start: Instant, stop: Instant): Option[Long] = {
    StopWatch.start
    var _count: Option[Long] = None
    try {
      val query = Flux.from(bucket)
        .range(start, stop)
        .filter(Restrictions.measurement().equal(measurement))
        .count
      val results = _client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        // [TIPS] Get real row size, or not field row
        _count = Some(record.getValue.toString.toLong)
      }))
      Await.result(sink, Duration.Inf)
      StopWatch.stop
      logInfo(s"Get count between time range cost time: ${StopWatch.cost}ms")
      _count
    } catch {
      case e: Exception =>
        logError("Get count between time range failed!", e)
        _count
    }
  }

  /**
   * Read record data within a given time interval about [t, t + δt] (δt -> 0) from the InfluxDB.
   *
   * [Question-1] Is the `range` method algorithm complexity convergent here.
   * [Question-2] How to solve the `data skew` problem for time series data if the probability obeys the `Gaussian distribution`?
   *
   * @param bucket
   * @param measurement
   * @param start  start instant
   * @param stop   end   instant
   * @return Iterator[FluxRecord]
   */
  def queryByTimeRange(bucket: String, measurement: String, start: Instant, stop: Instant) = {
    StopWatch.start
    val elements = scala.collection.mutable.ArrayBuffer[FluxRecord]()
    try {
      val query = Flux.from(bucket)
        .range(start, stop)
        .filter(Restrictions.measurement().equal(measurement))
      val results = _client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](elements.append(_)))
      Await.result(sink, Duration.Inf)
      StopWatch.stop
      logInfo(s"Query between time range cost time: ${StopWatch.cost}ms")
      elements.toIterator
    } catch {
      case e: Exception =>
        logError("Query between time range failed!", e)
        elements.toIterator
    }
  }

  /**
   * Write Line Protocol records into specified bucket.
   *
   * @param bucket
   *
   * @param record
   *          e.g. "sensor,sid=1 pm25_aqi=101,pm10_aqi=172,no2_aqi=131,temperature=-1,pressure=925,humidity=210,wind=4,weather=2"
   */
  def writeRecordIterator(bucket: String, records: Iterator[String]) = {
    StopWatch.start
    try {
      val source = Source.fromIterator(() => records)
      val sink = _client.getWriteScalaApi.writeRecord(Some(WritePrecision.NS), Some(bucket))
      val materialized = source.toMat(sink)(Keep.right)
      Await.result(materialized.run(), Duration.Inf)
      StopWatch.stop
      logInfo(s"Write record iterator cost time: ${StopWatch.cost}ms")
    } catch {
      case e: Exception =>
        logError("Write record iterator failed!", e)
    }
  }

  /**
   * Write Line Protocol records into specified bucket.
   *
   * @param bucket
   * @param pointRecords
   *          e.g.
   *              val point = Point
   *                .measurement("mem")
   *                .addTag("host", "host1")
   *                .addField("used_percent", 23.43234543)
   *                .time(Instant.now(), WritePrecision.NS)
   */
  def writePointIterator(bucket: String, pointRecords: Iterator[Point]) = {
    StopWatch.start
    try {
      val source = Source.fromIterator(() => pointRecords)
      val sink = _client.getWriteScalaApi.writePoint()
      val materialized = source.toMat(sink)(Keep.right)
      Await.result(materialized.run(), Duration.Inf)
      StopWatch.stop
      logInfo(s"Write record iterator cost time: ${StopWatch.cost}ms")
    } catch {
      case e: Exception =>
        logError("Write record iterator failed!", e)
    }
  }
}

object FluxClient {

  private val RANGE_START = -1L

  private val ACTOR_SYSTEM_NAME = "influxdb-client-actor-system"

  def apply(options: InfluxDBClientOptions) = new FluxClient(options)
}
