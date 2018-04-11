package service

import dao.hbase.{HBaseConnectionManager, HBaseDao}
import dao.parquet.ParquetDAO
import org.apache.hadoop.hbase.client.Connection
import spark.SparkSessionManager

/**
  *
  */
trait EnterpriseAssemblerService extends HBaseConnectionManager with SparkSessionManager{

  def loadFromCsv{
    withSpark{ implicit SparkSession => ParquetDAO.parquetToHFile(SparkSession)}
  }

  def loadFromHFile = withHbaseConnection { implicit connection: Connection => HBaseDao.loadHFiles}

}
