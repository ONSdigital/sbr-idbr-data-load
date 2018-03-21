package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import dao.csv.CsvDAO
import org.apache.hadoop.hbase.client.Connection
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{

  def loadFromParquet{
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile }
  }

  def loadFromCsv{
    withSpark{ implicit SparkSession => CsvDAO.csvToParquet}
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
