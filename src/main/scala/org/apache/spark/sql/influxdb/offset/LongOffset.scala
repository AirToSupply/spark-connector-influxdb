package org.apache.spark.sql.influxdb.offset

import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

case class LongOffset(offset: Long) extends Offset {

  override val json = offset.toString
  def +(increment: Long): LongOffset = new LongOffset(offset + increment)
  def -(decrement: Long): LongOffset = new LongOffset(offset - decrement)
}
object LongOffset {

  def apply(offset: SerializedOffset) : LongOffset = new LongOffset(offset.json.toLong)

  def convert(offset: Offset): Option[LongOffset] = offset match {
    case lo: LongOffset => Some(lo)
    case so: SerializedOffset => Some(LongOffset(so))
    case _ => None
  }
}
