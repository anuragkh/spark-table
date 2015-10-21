package amplab.spark

import org.apache.spark.sql.{DataFrame, SQLContext}

package object table {

  implicit class TableContext(sqlContext: SQLContext) {
    def tableFile(filePath: String): DataFrame = {
      sqlContext.baseRelationToDataFrame(TableRelation(filePath)(sqlContext))
    }
  }

  implicit class TableDataFrame(dataFrame: DataFrame) {
    def saveAsTableFile(path: String): Unit = {
      dataFrame.write.format("parquet").save(path)
    }
  }

}
