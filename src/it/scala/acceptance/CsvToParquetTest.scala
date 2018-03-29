/**
  * Created by chohab on 20/03/2018.
  */

package acceptance


import org.scalatest._
import spark.SparkSessionManager
import global.Configs._
import org.apache.spark.sql.SparkSession
import service.EnterpriseAssemblerService


class CsvToParquetTest extends FreeSpec with Matchers with BeforeAndAfterAll with SparkSessionManager{

  override def beforeAll() = {
    global.Configs.conf.set("files.parquet","src/main/resources/data/test.parquet")
  }

  "A comparison with the golden master" - {
    "should return zero differences" in {

      val assembler = new EnterpriseAssemblerService{}
      assembler.loadFromCsv

      val spark = SparkSession.builder().master("local[4]").appName("idbr enterprise assembler").getOrCreate()

      val df1 = spark.read.parquet(PATH_TO_PARQUET).repartition(1).sort("entref")
      val df2 = spark.read.parquet("src/it/resources/data/sample.parquet").repartition(1).sort("entref")

      val difference = df1.except(df2)
      difference.count() shouldBe 0


    }
  }
}