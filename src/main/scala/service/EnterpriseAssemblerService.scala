package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import org.apache.hadoop.hbase.client.Connection
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{
  import global.Configs._

  def loadFromParquet{
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile }
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
