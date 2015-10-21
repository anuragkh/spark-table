package amplab.spark.table

import java.io.FileWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{UTF8String, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

case class TableRelation(location: String, userSchema: StructType = null)(@transient val sqlContext: SQLContext)
  extends BaseRelation with PrunedFilteredScan {

  val tableDF: DataFrame = sqlContext.read.format("parquet").load(location)

  private def stringify(value: Any): String = value match {
    case s: String => {
      s"\'$value\'"
    }
    case u: UTF8String => {
      s"\'$value\'"
    }
    case _ => {
      value.toString
    }
  }

  private def filterString(filter: Filter): String = filter match {
    case EqualTo(attribute, value) => s"$attribute = ${stringify(value)}"
    case LessThan(attribute, value) => s"$attribute < ${stringify(value)}"
    case LessThanOrEqual(attribute, value) => s"$attribute <= ${stringify(value)}"
    case GreaterThan(attribute, value) => s"$attribute > ${stringify(value)}"
    case GreaterThanOrEqual(attribute, value) => s"$attribute >= ${stringify(value)}"

    /** Rest are unsupported for now */
    case _ => throw new UnsupportedOperationException(s"Unsupported filter")
  }

  override def schema: StructType = if (userSchema == null) tableDF.schema else userSchema

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Logging
    val logger = new FileWriter("log", true)
    var prunedFilteredDF = tableDF

    if (requiredColumns.length == 0) return prunedFilteredDF.rdd

    prunedFilteredDF = prunedFilteredDF.select(requiredColumns(0), requiredColumns.slice(1, requiredColumns.length):_*)
    logger.write(s"Column selectivity: ${requiredColumns.length}/${schema.length}\n")
    val rowBefore = prunedFilteredDF.count()
    filters.foreach(filter => {
      val filterStr = filterString(filter)
      println("Filter String = " + filterStr)
      val before = prunedFilteredDF.count()
      prunedFilteredDF = prunedFilteredDF.filter(filterStr)
      val after = prunedFilteredDF.count()
      logger.write(s"$filterStr: Filter selectivity: $after/$before (${after.toDouble/before.toDouble})\n")
    }
    )
    val rowAfter = prunedFilteredDF.count()
    logger.write(s"Row selectivity: $rowAfter/$rowBefore (${rowAfter.toDouble/rowBefore.toDouble})\n")
    logger.close()

    prunedFilteredDF.rdd
  }
}
