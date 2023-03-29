package org.apache.spark.sql.influxdb.util

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.influxdb.client.InfluxDBClientOptions
import com.influxdb.client.scala.{InfluxDBClientScala, InfluxDBClientScalaFactory}
import com.influxdb.query.FluxRecord
import com.influxdb.query.dsl.Flux
import com.influxdb.query.dsl.functions.restriction.Restrictions
import org.apache.spark.internal.Logging

import java.time.Instant
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.Duration

object Fluxs extends Logging {

  private val RANGE_START = -1L

  private def _createClient(options: InfluxDBClientOptions) =
    InfluxDBClientScalaFactory.create(options)

  private def _closeClient(client: InfluxDBClientScala) = client.close()

  private def _createActorSystem = ActorSystem(UUID.randomUUID().toString)

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

  def minimumTime(options: InfluxDBClientOptions)
                 (bucket: String, measurement: String): Option[Instant] = {
    implicit val system = _createActorSystem
    val client = _createClient(options)
    var _time: Option[Instant] = None
    try {
      val query = Flux.from(bucket)
        .range(RANGE_START)
        .filter(Restrictions.measurement().equal(measurement))
        .first()
      val results = client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        _time = Some(record.getTime)
      }))
      Await.result(sink, Duration.Inf)
      _time
    } catch {
      case e: Exception =>
        logError("Get minimum _time failed!", e)
        _closeClient(client)
        _terminateActorSystem(system)
        _time
    } finally {
      _closeClient(client)
      _terminateActorSystem(system)
    }
  }

  def maximumTime(options: InfluxDBClientOptions)
                 (bucket: String, measurement: String): Option[Instant] = {
    implicit val system = _createActorSystem
    val client = _createClient(options)
    var _time: Option[Instant] = None
    try {
      val query = Flux.from(bucket)
        .range(RANGE_START)
        .filter(Restrictions.measurement().equal(measurement))
        .last()
      val results = client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        _time = Some(record.getTime)
      }))
      Await.result(sink, Duration.Inf)
      _time
    } catch {
      case e: Exception =>
        logError("Get maximum _time failed!", e)
        _closeClient(client)
        _terminateActorSystem(system)
        _time
    } finally {
      _closeClient(client)
      _terminateActorSystem(system)
    }
  }

  def count(options: InfluxDBClientOptions)
           (bucket: String, measurement: String, start: Instant, stop: Instant): Option[Long] = {
    implicit val system = _createActorSystem
    val client = _createClient(options)
    var _count: Option[Long] = None
    try {
      val query = Flux.from(bucket)
        .range(start, stop)
        .filter(Restrictions.measurement().equal(measurement))
        .count
      val results = client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](record => {
        // [TIPS] Get real row size, or not field row
        _count = Some(record.getValue.toString.toLong)
      }))
      Await.result(sink, Duration.Inf)
      _count
    } catch {
      case e: Exception =>
        logError("Get count between time range failed!", e)
        _closeClient(client)
        _terminateActorSystem(system)
        _count
    } finally {
      _closeClient(client)
      _terminateActorSystem(system)
    }
  }

  def queryByTimeRange(options: InfluxDBClientOptions)
                      (bucket: String, measurement: String, start: Instant, stop: Instant) = {
    implicit val system = _createActorSystem
    val client = _createClient(options)
    val elements = scala.collection.mutable.ArrayBuffer[FluxRecord]()
    try {
      val query = Flux.from(bucket)
        .range(start, stop)
        .filter(Restrictions.measurement().equal(measurement))
      val results = client.getQueryScalaApi().query(query.toString)
      val sink = results.runWith(Sink.foreach[FluxRecord](elements.append(_)))
      Await.result(sink, Duration.Inf)
      elements.toIterator
    } catch {
      case e: Exception =>
        logError("Query between time range failed!", e)
        _closeClient(client)
        _terminateActorSystem(system)
        elements.toIterator
    } finally {
      _closeClient(client)
      _terminateActorSystem(system)
    }
  }
}
