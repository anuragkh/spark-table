package amplab.spark.table

import java.io.{File, IOException}

import com.google.common.io.Files
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest._

import scala.util.Random

object TestUtils {

  /**
   * This function deletes a file or a directory with everything that's in it.
   */
  def deleteRecursively(file: File) {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  def castToType(elem: String, dataType: DataType): Any = {
    if (elem == "NULL") return null
    dataType match {
      case BooleanType => elem.equals("1")
      case ByteType => elem.toByte
      case ShortType => elem.toShort
      case IntegerType => elem.toInt
      case LongType => elem.toLong
      case FloatType => elem.toFloat
      case DoubleType => elem.toDouble
      case _: DecimalType => Decimal(new java.math.BigDecimal(elem))
      case StringType => elem
      case other => throw new IllegalArgumentException(s"Unexpected type $dataType.")
    }
  }
}

class TableSQLSuite extends FunSuite with BeforeAndAfterAll {
  val rawTable = getClass.getResource("/table.dat").getFile
  val parquetTable = rawTable + ".parquet"
  val citiesTable = getClass.getResource("/cities.dat").getFile
  val testSchema = StructType(Seq(
    StructField("Name", StringType, nullable = false),
    StructField("Length", IntegerType, nullable = true),
    StructField("Area", DoubleType, nullable = false),
    StructField("Airport", BooleanType, nullable = true)))

  override def beforeAll(): Unit = {
    val baseRDD = TestSQLContext.sparkContext.textFile(getClass.getResource("/table.dat").getFile)
      .map(_.split('|').toSeq)
    val firstRecord = baseRDD.first()
    val schema = StructType(firstRecord.map(StructField(_, StringType)))
    val tableRDD = baseRDD.filter(_ != firstRecord).map(Row.fromSeq(_))
    TestSQLContext.createDataFrame(tableRDD, schema).write.format("parquet").save(parquetTable)
  }

  def createTestDF(schema: StructType = testSchema): (DataFrame, DataFrame) = {
    val cityRDD = sparkContext.textFile(citiesTable)
      .map(_.split(','))
      .map { t =>
      Row.fromSeq(Seq.tabulate(schema.size)(i => TestUtils.castToType(t(i), schema(i).dataType)))
    }
    val df = TestSQLContext.createDataFrame(cityRDD, schema)

    val tempDir = Files.createTempDir()
    val tableDir = tempDir + "/table"
    df.write.format("amplab.spark.table").save(tableDir)
    val loadedDF = TestSQLContext.tableFile(tableDir)
    (df, loadedDF) // (expected, actual: table loaded)
  }

  test("dsl test") {
    val results = TestSQLContext
      .tableFile(parquetTable)
      .select("shipmode")
      .collect()

    assert(results.length === 1000)
  }

  test("prunes") {
    val (cityDataFrame, loadedDF) = createTestDF(testSchema)

    def checkPrunes(columns: String*) = {
      val expected = cityDataFrame.select(columns.map(cityDataFrame(_)): _*).collect()
      val actual = loadedDF.select(columns.map(loadedDF(_)): _*).collect()
      assert(actual.length === expected.length)
      expected.foreach(row => assert(row.toSeq.length == columns.length))
    }

    checkPrunes("Name")
    checkPrunes("Length")
    checkPrunes("Area")
    checkPrunes("Airport")
    checkPrunes("Name", "Length")
    checkPrunes("Area", "Airport")
    checkPrunes("Name", "Area", "Airport")
    checkPrunes("Name", "Length", "Area", "Airport")
  }

  test("filters") {
    def checkFilters[T](expectedDF: DataFrame, actualDF: DataFrame,
                        column: String, makeThresholds: => Seq[T]) = {
      def check(column: String, op: String, threshold: T) = {
        try {
          val expected = expectedDF.filter(s"$column $op $threshold")
          val actual = actualDF.filter(s"$column $op $threshold")
          assert(actual.count() === expected.count(), s"fails $op $threshold on column $column")
        } catch {
          case e: Exception =>
            println(s"****query: '$column $op $threshold'")
            throw e
        }
      }
      for (threshold <- makeThresholds) {
        for (op <- Seq("<", "<=", ">", ">=", "=")) {
          check(column, op, threshold)
        }
      }
    }

    val rand = new Random()

    // string, integer, double, boolean columns
    val (cityDataFrame, loadedDF) = createTestDF(testSchema)

    checkFilters(cityDataFrame, loadedDF, "Name",
      Seq("''", "'Z'", "'Las Vegas'", "'Aberdeen'", "'Bronxville'"))
    checkFilters(cityDataFrame, loadedDF, "Length",
      Seq.fill(2)(rand.nextInt(1000)))
    checkFilters(cityDataFrame, loadedDF, "Area",
      Seq(-1, 0.0, 999.2929, 1618.15, 9, 659) ++ Seq.fill(2)(rand.nextDouble() * 1000))
    checkFilters(cityDataFrame, loadedDF, "Airport",
      Seq(false, true))

  }

  test("test load and save") {
    // Test if load works as expected
    val df = TestSQLContext.read.format("amplab.spark.table").load(parquetTable)
    assert(df.count == 1000)

    // Test if save works as expected
    val tempSaveDir = Files.createTempDir().getAbsolutePath
    TestUtils.deleteRecursively(new File(tempSaveDir))
    df.write.format("amplab.spark.table").save(tempSaveDir)
    val newDf = TestSQLContext.read.format("amplab.spark.table").load(tempSaveDir)
    assert(newDf.count == 1000)
  }

}
