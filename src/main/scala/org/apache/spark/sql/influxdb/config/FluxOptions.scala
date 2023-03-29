package org.apache.spark.sql.influxdb.config

import com.influxdb.client.InfluxDBClientOptions

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

  def createInfluxDBClientOptions(options: Map[String, String]) = InfluxDBClientOptions.builder()
    .url(s"http://${host(options)}:${port(options)}")
    .authenticate(user(options), password(options).toCharArray)
    .org(org(options))
    .build()
}
