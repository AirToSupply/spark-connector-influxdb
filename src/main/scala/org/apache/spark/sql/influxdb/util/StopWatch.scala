package org.apache.spark.sql.influxdb.util

object StopWatch {

  private var _start: Long = _

  private var _stop: Long = _

  def start = _start = System.currentTimeMillis

  def stop = _stop = System.currentTimeMillis

  def cost = _stop - _start
}