package org.apache.spark.sql.influxdb.config

import com.influxdb.client.InfluxDBClientOptions

import java.time.ZoneId

object FluxOptions {

  /**
   * server host
   */
  val OPTION_SERVER_HOST = "host"
  private val OPTION_SERVER_HOST_VALUE = "localhost"

  /**
   * server port
   */
  val OPTION_SERVER_PORT = "port"
  private val OPTION_SERVER_PORT_VALUE = "8086"

  /**
   * server username
   */
  val OPTION_SERVER_USER = "user"

  /**
   * server password
   */
  val OPTION_SERVER_PASSWORD = "password"

  /**
   * org
   */
  val OPTION_ORG = "org"

  /**
   * bucket
   */
  val OPTION_BUCKET = "bucket"

  /**
   * measurement
   */
  val OPTION_MEASUREMENT = "measurement"

  /**
   * delta time (Unit: ms)
   */
  val OPTION_DELTA_TIME = "delta-time"
  private val OPTION_DELTA_TIME_VALUE = "1000"

  val OPTION_TIME_ZONE = "time-zone"
  private val OPTION_TIME_ZONE_VALUE = ZoneId.systemDefault().toString

  /**
   * batch size
   */
  val OPTION_BATCH_SIZE = "batchSize"
  private val OPTION_BATCH_SIZE_VALUE = "1000"

  /**
   * partition number
   */
  val OPTION_PARTITION_NUM = "numPartitions"

  def host(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_HOST, OPTION_SERVER_HOST_VALUE)

  def port(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_PORT, OPTION_SERVER_PORT_VALUE)

  def user(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_USER, {
    throw new IllegalArgumentException(s"Option '${OPTION_SERVER_USER}' must be require!")
  })

  def password(options: Map[String, String]) = options.getOrElse(OPTION_SERVER_PASSWORD, {
    throw new IllegalArgumentException(s"Option '${OPTION_SERVER_PASSWORD}' must be require!")
  })

  def org(options: Map[String, String]) = options.getOrElse(OPTION_ORG, {
    throw new IllegalArgumentException(s"Option '${OPTION_ORG}' must be require!")
  })

  def bucket(options: Map[String, String]) = options.getOrElse(OPTION_BUCKET, {
    throw new IllegalArgumentException(s"Option '${OPTION_BUCKET}' must be require!")
  })

  def measurement(options: Map[String, String]) = options.getOrElse(OPTION_MEASUREMENT, {
    throw new IllegalArgumentException(s"Option '${OPTION_MEASUREMENT}' must be require!")
  })

  def deltaTime(options: Map[String, String]) =
    options.getOrElse(OPTION_DELTA_TIME, OPTION_DELTA_TIME_VALUE).toLong

  def timeZone(options: Map[String, String]) = options.getOrElse(OPTION_TIME_ZONE, OPTION_TIME_ZONE_VALUE)

  def batchSize(options: Map[String, String]) = options.getOrElse(OPTION_BATCH_SIZE, OPTION_BATCH_SIZE_VALUE).toInt

  def numPartitions(options: Map[String, String]) = options.get(OPTION_PARTITION_NUM).map(_.toInt)

  def createInfluxDBClientOptions(options: Map[String, String]) = InfluxDBClientOptions.builder()
    .url(s"http://${host(options)}:${port(options)}")
    .authenticate(user(options), password(options).toCharArray)
    .org(org(options))
    .build()
}
