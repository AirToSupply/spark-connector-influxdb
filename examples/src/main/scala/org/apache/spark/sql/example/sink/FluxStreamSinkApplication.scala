package org.apache.spark.sql.example.sink

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.influxdb.provider.FluxSinkProvider
import org.apache.spark.sql.streaming.OutputMode

/**
 * Continuous writing to InfluxDB from `nc`
 *
 * You need to enter `nc -lk 7777` on the terminal and it is started an interactive service to simulate
 * the input of raw data record based on `line protocol` about to InfluxDB.
 *
 * The input data may be as follows:
 *
 *   sensor,sid=1 pm25_aqi=1,pm10_aqi=72,no2_aqi=31,temperature=-1,pressure=925,humidity=46,wind=4,weather=2 1723456926000000000
 *   sensor,sid=1 pm25_aqi=-1,pm10_aqi=74,no2_aqi=33,temperature=0,pressure=625,humidity=86,wind=4,weather=2 1723456927000000000
 *   sensor,sid=1 pm25_aqi=2,pm10_aqi=71,no2_aqi=32,temperature=-5,pressure=985,humidity=76,wind=4,weather=2 1723456928000000000
 *   sensor,sid=1 pm25_aqi=-4,pm10_aqi=70,no2_aqi=34,temperature=4,pressure=885,humidity=100,wind=4,weather=2 1723456929000000000
 *   sensor,sid=1 pm25_aqi=7,pm10_aqi=72,no2_aqi=31,temperature=-1,pressure=925,humidity=96,wind=4,weather=2 1723456930000000000
 */
object FluxStreamSinkApplication {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("source-nc-sink-influxdb")
      .master("local[*]")
      .getOrCreate()

    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 7777)
      .load()
      .writeStream
      .format(classOf[FluxSinkProvider].getName)
      .outputMode(OutputMode.Append())
      .option("host", "localhost")
      .option("port", "8086")
      // username and password
      .option("user", "influxdb")
      .option("password", "influxdb")
      // access token
      // .option("token", "jTcun3c8QgDrA-7dKtSegn89p7mt252noulSbMY10tekmpOMI4iEFcM0E77YTouU4OZt5K_98mX6DW_8FFe2IA==")
      .option("org", "org")
      .option("bucket", "test_bucket")
      .option("checkpointLocation", "file:///tmp/source_nc_sink_influxdb_checkpoint")
      .start()
      .awaitTermination()
  }
}
