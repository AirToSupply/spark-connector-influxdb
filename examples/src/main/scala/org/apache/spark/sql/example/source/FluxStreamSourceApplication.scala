package org.apache.spark.sql.example.source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.influxdb.provider.FluxSourceProvider
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

/**
 * Continuous reading of InfluxDB data and printing to the console.
 *
 * Prepare the data as follows:
 *
 * measurement:
 *   sensor
 *
 * raw records based on line protocol:
 *   sensor,sid=1 pm25_aqi=1,pm10_aqi=72,no2_aqi=31,temperature=-1,pressure=925,humidity=46,wind=4,weather=2
 *   sensor,sid=1 pm25_aqi=2,pm10_aqi=75,no2_aqi=31,temperature=-1,pressure=924,humidity=47,wind=4,weather=2
 *   sensor,sid=1 pm25_aqi=3,pm10_aqi=100,no2_aqi=31,temperature=-1,pressure=928,humidity=48,wind=4,weather=2
 *   sensor,sid=1 pm25_aqi=4,pm10_aqi=200,no2_aqi=31,temperature=-1,pressure=929,humidity=99,wind=4,weather=2
 *   sensor,sid=1 pm25_aqi=20,pm10_aqi=210,no2_aqi=32,temperature=2,pressure=945,humidity=86,wind=4,weather=5
 *   sensor,sid=1 pm25_aqi=23,pm10_aqi=200,no2_aqi=32,temperature=1,pressure=915,humidity=16,wind=6,weather=4
 *   sensor,sid=1 pm25_aqi=24,pm10_aqi=200,no2_aqi=32,temperature=2,pressure=885,humidity=98,wind=6,weather=5
 *   sensor,sid=1 pm25_aqi=25,pm10_aqi=100,no2_aqi=33,temperature=4,pressure=785,humidity=29,wind=6,weather=4
 */
object FluxStreamSourceApplication {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("source-influxdb-sink-console")
      .master("local[*]")
      .getOrCreate()

    spark.readStream
      .format(classOf[FluxSourceProvider].getName)
      .option("host", "localhost")
      .option("port", "8086")
      .option("user", "influxdb")
      .option("password", "influxdb")
      .option("org", "org")
      .option("bucket", "test_bucket")
      .option("measurement", "sensor")
      .load()
      .writeStream
      .format("console")
      .outputMode("append")
      .option("numRows", Int.MaxValue)
      .option("truncate", false)
      .option("checkpointLocation", "/tmp/source_influxdb_sink_console_checkpoint")
      .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
      .start()
      .awaitTermination()
  }
}
