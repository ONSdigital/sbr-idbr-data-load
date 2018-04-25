package dao.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import scala.util.Try

/**
  *
  */
object HBaseDao {
  import global.Configs._

  def loadHFiles(implicit connection:Connection) ={
    loadLinksHFile
    loadEnterprisesHFile
    loadLouHFile
  }

  def loadLinksHFile(implicit connection:Connection) = wrapTransaction(HBASE_LINKS_TABLE_NAME, Try(conf.getStrings("hbase.table.namespace").head).toOption){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PATH_TO_LINKS_LOU_HFILE), admin,table,regionLocator)
    bulkLoader.doBulkLoad(new Path(PATH_TO_LINKS_ENT_HFILE), admin,table,regionLocator)
  }

  def loadEnterprisesHFile(implicit connection:Connection) = wrapTransaction(HBASE_ENTERPRISE_TABLE_NAME,Try(conf.getStrings("hbase.table.namespace").head).toOption){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PATH_TO_ENT_HFILE), admin,table,regionLocator)
  }

  def loadLouHFile(implicit connection:Connection) = wrapTransaction(HBASE_LOU_TABLE_NAME,Try(conf.getStrings("hbase.table.namespace").head).toOption){ (table, admin) =>
    val bulkLoader = new LoadIncrementalHFiles(connection.getConfiguration)
    val regionLocator = connection.getRegionLocator(table.getName)
    bulkLoader.doBulkLoad(new Path(PATH_TO_LOU_HFILE), admin,table,regionLocator)
  }

  private def wrapTransaction(tableName:String,nameSpace:Option[String])(action:(Table,Admin) => Unit)(implicit connection:Connection){
    val tn = nameSpace.map(ns => TableName.valueOf(ns, tableName)).getOrElse(TableName.valueOf(tableName))
    val table: Table = connection.getTable(tn)
    val admin = connection.getAdmin
    setJob(table)
    action(table,admin)
    admin.close
    table.close
  }

  private def setJob(table:Table)(implicit connection:Connection){
    val job = Job.getInstance(connection.getConfiguration)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
  }

}
