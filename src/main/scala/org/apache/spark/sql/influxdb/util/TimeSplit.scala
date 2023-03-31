package org.apache.spark.sql.influxdb.util

import java.time.Instant

case class TimeSlice(start: Instant, stop: Instant)

object TimeSpliter {

  def slice(start: Instant, stop: Instant, split: Int = 4): Array[TimeSlice] = {
    if (split < 1) {
      throw new IllegalArgumentException("The time slice number must be greater than zero!")
    }
    val (startNanos, stopNanos) = (Nanos.toNano(start), Nanos.toNano(stop))
    if (startNanos > stopNanos) {
      throw new IllegalArgumentException("The start time cannot be greater than the end time!")
    }
    split match {
      case 1 =>
        Array[TimeSlice](TimeSlice(start, stop))
      case _ =>
        val fisrtIndex = 1
        val lastIndex = split
        val delta = (stopNanos - startNanos) / split
        var _left: Long = 0L
        (fisrtIndex to lastIndex).map { index =>
          val (lowerBound, upperBound) = index match {
            case _@idx if idx == fisrtIndex =>
              val left = startNanos
              val right = startNanos + delta
              _left = right
              (left, right)
            case _@idx if idx == lastIndex =>
              val left = _left
              val right = stopNanos
              (left, right)
            case _ =>
              val left = _left
              val right = _left + delta
              _left = right
              (left, right)
          }
          TimeSlice(Nanos.parse(lowerBound), Nanos.parse(upperBound))
        }.toArray
    }
  }
}
