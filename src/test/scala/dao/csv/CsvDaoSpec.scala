package dao.csv

import model.domain._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll,BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._

import scala.reflect.io.File
/**
  *
  */
class CsvDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData{

  import global.Configs._


  private val entLinkHFilePath = "src/test/resources/data/links/enterprise"
  private val entHFilePath = "src/test/resources/data/enterprise"
  private val entCsvFilePath = "src/test/resources/data/idbr.csv"

  private val louLinkHFilePath = "src/test/resources/data/links/lou"
  private val louHFilePath = "src/test/resources/data/lou"
  private val louCsvFilePath = "src/test/resources/data/sampleLocal.csv"

  override def beforeAll() = {

    conf.set("enterprise.data.timeperiod", "default")

    updateConf(Array[String]("links", "sbr_dev_db", entLinkHFilePath, louLinkHFilePath,
      "ent", "sbr_dev_db", entHFilePath, "lou", "sbr_dev_db", louHFilePath,
      louCsvFilePath, entCsvFilePath, "localhost", "2181", "201802"
    ))
  }

  override def afterEach() = {
    File(entLinkHFilePath).deleteRecursively()
    File(louLinkHFilePath).deleteRecursively()
    File(entHFilePath).deleteRecursively()
    File(louHFilePath).deleteRecursively()
  }

  "assembler" should {
    "create hfiles populated with expected enterprise data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

      CsvDAO.csvToHFile

      private val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHFilePath).collect.toList.sortBy(_.ern)
      private val expected: List[Enterprise] = testEnterprise(actual).sortBy(_.ern).toList
      actual shouldBe expected

      spark.close()
    }
  }

  "assembler" should {
    "create hfiles populated with expected local unit data" in {

      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

      CsvDAO.csvToHFile

      private val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHFilePath).collect.toList.sortBy(_.lurn)
      private val expected: List[LocalUnit] = testLocalUnit(actual).sortBy(_.lurn).toList
      actual shouldBe expected

      spark.close()
    }
  }
}