package org.apache.spark.sql.influxdb.util

import java.time.temporal.ChronoField
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.concurrent.TimeUnit

object Nanos {

  private val NANO_OF_SECONDS = scala.math.pow(10, 9).toLong

  private val MILLIS_OF_SECONDS = scala.math.pow(10, 6).toLong

  /**
   * Parse UTC string format
   *
   * @param utc utc string e.g. 2023-03-27T06:13:36.725154800Z
   *
   * @return
   */
  def parse(utc: String) = Instant.parse(utc)

  /**
   * Parse nano long format
   *
   * @param nano e.g. 1679897616725154800L
   *
   * @return
   */
  def parse(nano: Long) = Instant.ofEpochSecond(nano / NANO_OF_SECONDS, nano % NANO_OF_SECONDS)

  /**
   * Transfer nano seconds
   *
   * @param instant
   * @return
   */
  def toNano(instant: Instant): Long = {
    val (seconds, nanoSeconds) = toNanoSeconds(instant)
    seconds * NANO_OF_SECONDS + nanoSeconds
  }

  def toNano(instant: ZonedDateTime): Long = {
    val (seconds, nanoSeconds) = toNanoSeconds(instant)
    seconds * NANO_OF_SECONDS + nanoSeconds
  }

  def millisToNano(millis: Long) = millis * MILLIS_OF_SECONDS

  def secondsToNano(seconds: Long) = seconds * NANO_OF_SECONDS

  /**
   * Transfer nano seconds (second + nano of second)
   *
   * e.g. 1679897616725154800L => (1679897616L, 725154800L)
   *
   * @param instant
   *
   * @return
   */
  def toNanoSeconds(instant: Instant) = (
    instant.getLong(ChronoField.INSTANT_SECONDS),
    instant.getLong(ChronoField.NANO_OF_SECOND))

  def toNanoSeconds(instant: ZonedDateTime) = (
    instant.getLong(ChronoField.INSTANT_SECONDS) + instant.getOffset.getTotalSeconds.toLong,
    instant.getLong(ChronoField.NANO_OF_SECOND))

  def add(instant: Instant, delta: Long, unit: TimeUnit = TimeUnit.MILLISECONDS) =
    parse(toNano(instant) + unit.toNanos(delta))

  /**
   * Transfer nano by zone
   *
   * @param instant
   * @param zoneId
   *          refer to http://www.taodudu.cc/news/show-4177074.html
   * @return
   */
  def toZone(instant: Instant, zoneId: String = ZoneId.systemDefault().toString) =
    parse(toNano(instant.atZone(ZoneId.of(zoneId))))
}
