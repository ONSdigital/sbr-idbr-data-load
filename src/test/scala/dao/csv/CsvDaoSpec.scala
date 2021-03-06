package dao.csv

import model.domain._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll,BeforeAndAfterEach, Matchers, WordSpecLike}
import spark.extensions.rdd.HBaseDataReader._
import global.Configs._

import scala.reflect.io.File
/**
  *
  */
class CsvDaoSpec extends WordSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach with TestData {

  private val entLinkHFilePath = "src/test/resources/data/links/enterprise"
  private val entHFilePath = "src/test/resources/data/enterprise"
  private val entCsvFilePath = "src/test/resources/data/ent.csv"

  private val louLinkHFilePath = "src/test/resources/data/links/lou"
  private val louHFilePath = "src/test/resources/data/lou"
  private val louCsvFilePath = "src/test/resources/data/lou.csv"

  private val reuLinkHFilePath = "src/test/resources/data/links/reu"
  private val reuHFilePath = "src/test/resources/data/reu"
  private val reuCsvFilePath = "src/test/resources/data/reu.csv"

  var sparkSession : SparkSession = _

  override def beforeAll() = {

    implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

    conf.set("enterprise.data.timeperiod", "default")

    updateConf(Array[String](
      "sbr_dev_db",
      "links", entLinkHFilePath, louLinkHFilePath, reuLinkHFilePath,
      "ent", entHFilePath,
      "lou", louHFilePath,
      "reu", reuHFilePath,
      louCsvFilePath, entCsvFilePath, reuCsvFilePath,
      "localhost", "2181", "201802"
    ))

    CsvDAO.csvToHFile
  }

  override def afterAll() = {
    File(entLinkHFilePath).deleteRecursively()
    File(louLinkHFilePath).deleteRecursively()
    File(reuLinkHFilePath).deleteRecursively()
    File(entHFilePath).deleteRecursively()
    File(louHFilePath).deleteRecursively()
    File(reuHFilePath).deleteRecursively()
    sparkSession.stop()
  }

//  "assembler" should {
//    "create hfiles populated with expected enterprise data" in {
//      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
//
//      val actual: List[Enterprise] = readEntitiesFromHFile[Enterprise](entHFilePath).collect.toList.sortBy(_.ern)
//      val expected: List[Enterprise] = testEnterprise(actual).sortBy(_.ern).toList
//      actual shouldBe expected
//    }
//  }
//
//  "assembler" should {
//    "create hfiles populated with expected local unit data" in {
//      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
//
//      val actual: List[LocalUnit] = readEntitiesFromHFile[LocalUnit](louHFilePath).collect.toList.sortBy(_.lurn)
//      val expected: List[LocalUnit] = testLocalUnit(actual).sortBy(_.lurn).toList
//      actual shouldBe expected
//    }
//  }

  "assembler" should {
    "create hfiles populated with expected reporting unit data" in {
      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()

      val actual: List[ReportingUnit] = readEntitiesFromHFile[ReportingUnit](reuHFilePath).collect.toList.sortBy(_.rurn)
      val expected: List[ReportingUnit] = testReportingUnit(actual).sortBy(_.rurn).toList
      actual shouldBe expected
    }
  }

  //unit links test


//  "assembler" should {
//    "create hfiles populated with expected enterprise unit links" in {
//
//      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
////
//      val actual: List[UnitLinks] = readEntitiesFromHFile[UnitLinks](entLinkHFilePath).collect.toList.sortBy(_.rowkey)
//      val expected: List[UnitLinks] = testEnterpriseLinks(actual).sortBy(_.rowkey).toList
//      actual shouldBe expected
//
//      spark.close()
//    }
//  }

//  "assembler" should {
//    "create hfiles populated with expected local unit links" in {
//
//      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
////
//      val actual: List[UnitLinks] = readEntitiesFromHFile[UnitLinks](louLinkHFilePath).collect.toList.sortBy(_.rowkey)
//      val expected: List[UnitLinks] = testEnterpriseLinks(actual).sortBy(_.rowkey).toList
//      actual shouldBe expected
//
//      spark.close()
//    }
//  }
//
//  "assembler" should {
//    "create hfiles populated with expected reporting unit links" in {
//
//      implicit val spark: SparkSession = SparkSession.builder().master("local[*]").appName("enterprise assembler").getOrCreate()
////
//      val actual: List[UnitLinks] = readEntitiesFromHFile[UnitLinks](entLinkHFilePath).collect.toList.sortBy(_.rowkey)
//      val expected: List[UnitLinks] = testEnterpriseLinks(actual).sortBy(_.rowkey).toList
//      actual shouldBe expected
//
//      spark.close()
//    }
//  }
}