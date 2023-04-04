package org.apache.spark.sql.influxdb.write

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.influxdb.config.FluxOptions
import org.apache.spark.sql.types.{BinaryType, DataType, StringType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

object LineProtocolWrite extends Logging {

  private val VALUE_ATTRIBUTE_NAME: String = "value"

  def write(
    sparkSession: SparkSession,
    data: DataFrame,
    parameters: Map[String, String]): Unit = {
    validateQuery(data.queryExecution.analyzed.output)

    val batchSize = FluxOptions.batchSize(parameters)
    val repartitionedDF = FluxOptions.numPartitions(parameters) match {
      case Some(n) if n <= 0 =>
        throw new IllegalArgumentException(
          s"Invalid value `$n` for parameter `${FluxOptions.OPTION_PARTITION_NUM}` " +
            s"in table writing via influxdb. The minimum value is 1.")
      case Some(n) if n < data.rdd.getNumPartitions => data.coalesce(n)
      case _ => data
    }

    repartitionedDF.queryExecution.toRdd.foreachPartition { iter =>
      LineProtocolWriteTask(TaskContext.get().partitionId(), parameters, batchSize).execute(iter)
    }
  }

  private def validateQuery(schema: Seq[Attribute]): Unit = {
    try {
      valueExpression(schema)
    } catch {
      case e: IllegalStateException => throw new AnalysisException(e.getMessage)
    }
  }

  private def valueExpression(schema: Seq[Attribute]): Expression = {
    expression(schema, VALUE_ATTRIBUTE_NAME, Seq(StringType, BinaryType)) {
      throw new IllegalStateException(s"Required attribute '${VALUE_ATTRIBUTE_NAME}' not found")
    }
  }

  private def expression(
    schema: Seq[Attribute],
    attrName: String,
    desired: Seq[DataType])(
    default: => Expression): Expression = {
    val expr = schema.find(_.name == attrName).getOrElse(default)
    if (!desired.exists(_.sameType(expr.dataType))) {
      throw new IllegalStateException(s"$attrName attribute unsupported type " +
        s"${expr.dataType.catalogString}. $attrName must be a(n) " +
        s"${desired.map(_.catalogString).mkString(" or ")}")
    }
    expr
  }

}
