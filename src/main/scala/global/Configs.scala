package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object Configs{

  val logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load()

  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hadoop.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hadoop.security.authentication","kerberos"))
  Try{config.getString("hbase.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hbase.security.authentication","kerberos"))
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.files.per.region"))
  }

  Try{config.getString("hbase.table.links.name")}.map(conf.set("hbase.table.links.name",_)).getOrElse(conf.set("hbase.table.links.name","LINKS"))
  Try{config.getString("hbase.table.links.column.family")}.map(conf.set("hbase.table.links.column.family",_)).getOrElse(conf.set("hbase.table.links.column.family","l"))
  Try{config.getString("hbase.table.links.namespace")}.map(conf.set("hbase.table.links.namespace",_)).getOrElse(conf.set("hbase.table.links.namespace","ons"))

  Try{config.getString("hbase.table.enterprise.name")}.map(conf.set("hbase.table.enterprise.name",_)).getOrElse(conf.set("hbase.table.enterprise.name","ENT"))
  Try{config.getString("hbase.table.enterprise.column.family")}.map(conf.set("hbase.table.enterprise.column.family",_)).getOrElse(conf.set("hbase.table.enterprise.column.family","d"))
  Try{config.getString("hbase.table.enterprise.namespace")}.map(conf.set("hbase.table.enterprise.namespace",_)).getOrElse(conf.set("hbase.table.enterprise.namespace","ons"))

  Try{config.getString("files.parquet")}.map(conf.set("files.parquet",_)).getOrElse(conf.set("files.parquet","src/main/resources/data/test.parquet"))
  Try{config.getString("files.lou.csv")}.map(conf.set("files.lou.csv",_)).getOrElse(conf.set("files.lou.csv","src/main/resources/data/sampleLocal.csv"))
  Try{config.getString("files.ent.csv")}.map(conf.set("files.ent.csv",_)).getOrElse(conf.set("files.ent.csv","src/main/resources/data/idbr.csv"))

  Try{config.getString("files.links.lou.hfile")}.map(conf.set("files.links.lou.hfile",_)).getOrElse(conf.set("files.links.lou.hfile","src/main/resources/data/links/lou/hfile"))
  Try{config.getString("files.links.ent.hfile")}.map(conf.set("files.links.ent.hfile",_)).getOrElse(conf.set("files.links.ent.hfile","src/main/resources/data/links/ent/hfile"))
  Try{config.getString("files.ent.hfile")}.map(conf.set("files.ent.hfile",_)).getOrElse(conf.set("files.ent.hfile","src/main/resources/data/enterprise/hfile"))
  Try{config.getString("files.lou.hfile")}.map(conf.set("files.lou.hfile",_)).getOrElse(conf.set("files.lou.hfile","src/main/resources/data/lou/hfile"))

  Try{config.getString("enterprise.data.timeperiod")}.map(conf.set("enterprise.data.timeperiod",_)).getOrElse(conf.set("enterprise.data.timeperiod","201802"))
  Try{config.getString("spark.deployment.mode")}.map(conf.set("spark.deployment.mode",_)).getOrElse(conf.set("spark.deployment.mode","local"))

   lazy val PATH_TO_PARQUET = conf.getStrings("files.parquet").head
   lazy val PATH_TO_LOU_CSV  = conf.getStrings("files.lou.csv").head
   lazy val PATH_TO_ENT_CSV = conf.getStrings("files.ent.csv").head
   lazy val PATH_TO_LINKS_LOU_HFILE =  conf.getStrings("files.links.lou.hfile").head
   lazy val PATH_TO_LINKS_ENT_HFILE =  conf.getStrings("files.links.ent.hfile").head
   lazy val PATH_TO_ENT_HFILE =  conf.getStrings("files.ent.hfile").head
   lazy val PATH_TO_LOU_HFILE =  conf.getStrings("files.lou.hfile").head

   lazy val HBASE_LINKS_TABLE_NAME = conf.getStrings("hbase.table.links.name").head
   lazy val HBASE_LINKS_TABLE_NAMESPACE = conf.getStrings("hbase.table.links.namespace").head
   lazy val HBASE_LINKS_COLUMN_FAMILY = conf.getStrings("hbase.table.links.column.family").head

   lazy val HBASE_ENTERPRISE_TABLE_NAME = conf.getStrings("hbase.table.enterprise.name").head
   lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head
   lazy val HBASE_ENTERPRISE_COLUMN_FAMILY = conf.getStrings("hbase.table.enterprise.column.family").head
   lazy val ENTERPRISE_DATA_TIMEPERIOD = conf.getStrings("enterprise.data.timeperiod").head

  def updateConf(args: Array[String]) = {

    Try(args(0)).map(conf.set("hbase.table.links.name", _)).getOrElse(Unit)
    Try(args(1)).map(conf.set("hbase.table.links.namespace", _)).getOrElse(Unit)
    Try(args(2)).map(conf.set("files.links.ent.hfile", _)).getOrElse(Unit)
    Try(args(3)).map(conf.set("files.links.lou.hfile", _)).getOrElse(Unit)

    Try(args(4)).map(conf.set("hbase.table.enterprise.name", _)).getOrElse(Unit)
    Try(args(5)).map(conf.set("hbase.table.enterprise.namespace", _)).getOrElse(Unit)
    Try(args(6)).map(conf.set("files.enterprise.hfile", _)).getOrElse(Unit)
    Try(args(7)).map(conf.set("files.lou.hfile", _)).getOrElse(Unit)

    Try(args(8)).map(conf.set("files.parquet", _)).getOrElse(Unit)
    Try(args(9)).map(conf.set("files.lou.csv", _)).getOrElse(Unit)
    Try(args(10)).map(conf.set("files.ent.csv", _)).getOrElse(Unit)

    Try(args(11)).map(conf.set("hbase.zookeeper.quorum", _)).getOrElse(Unit)
    Try(args(12)).map(conf.set("hbase.zookeeper.property.clientPort", _)).getOrElse(Unit)
    Try(args(13)).map(conf.set("enterprise.data.timeperiod", _)).getOrElse(Unit)

  }

}
