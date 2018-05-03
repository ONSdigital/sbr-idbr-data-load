package global


import com.typesafe.config._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.slf4j.LoggerFactory

import scala.util.Try


object Configs{

  val logger = LoggerFactory.getLogger(getClass)

  val config: Config = ConfigFactory.load()

  val entHfile = "files.enterprise.hfile"
  val entLinks = "files.links.ent.hfile"
  val louHfile = "files.lou.hfile"
  val louLinks = "files.links.lou.hfile"
  val dataDir = "src/main/resources/data"
  val nameSpace = "sbr_dev_db"

  val conf: Configuration = HBaseConfiguration.create()
  Try{config.getString("hadoop.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hadoop.security.authentication","kerberos"))
  Try{config.getString("hbase.security.authentication")}.map(conf.addResource).getOrElse(conf.set("hbase.security.authentication","kerberos"))
  Try{config.getString("hbase.kerberos.config")}.map(conf.addResource).getOrElse(logger.info("no config resource for kerberos specified"))
  Try{config.getString("hbase.path.config")}.map(conf.addResource).getOrElse {
    logger.info("no config resource for hbase specified. Default configs will be used")
    conf.set("hbase.zookeeper.quorum", config.getString("hbase.zookeper.url"))
    conf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", config.getInt("hbase.files.per.region"))
  }

  Try{config.getString("hbase.table.links.name")}.map(conf.set("hbase.table.links.name",_)).getOrElse(conf.set("hbase.table.links.name","links"))
  Try{config.getString("hbase.table.links.column.family")}.map(conf.set("hbase.table.links.column.family",_)).getOrElse(conf.set("hbase.table.links.column.family","l"))
  Try{config.getString("hbase.table.links.namespace")}.map(conf.set("hbase.table.links.namespace",_)).getOrElse(conf.set("hbase.table.links.namespace",nameSpace))
  Try{config.getString(entLinks)}.map(conf.set(entLinks,_)).getOrElse(conf.set(entLinks,s"$dataDir/links/ent/hfile"))
  Try{config.getString(louLinks)}.map(conf.set(louLinks,_)).getOrElse(conf.set(louLinks,s"$dataDir/links/lou/hfile"))

  Try{config.getString("hbase.table.enterprise.name")}.map(conf.set("hbase.table.enterprise.name",_)).getOrElse(conf.set("hbase.table.enterprise.name","enterprise"))
  Try{config.getString("hbase.table.enterprise.column.family")}.map(conf.set("hbase.table.enterprise.column.family",_)).getOrElse(conf.set("hbase.table.enterprise.column.family","d"))
  Try{config.getString("hbase.table.enterprise.namespace")}.map(conf.set("hbase.table.enterprise.namespace",_)).getOrElse(conf.set("hbase.table.enterprise.namespace",nameSpace))
  Try{config.getString(entHfile)}.map(conf.set(entHfile,_)).getOrElse(conf.set(entHfile,s"$dataDir/enterprise/hfile"))

  Try{config.getString("hbase.table.lou.name")}.map(conf.set("hbase.table.lou.name",_)).getOrElse(conf.set("hbase.table.lou.name","lou"))
  Try{config.getString("hbase.table.lou.column.family")}.map(conf.set("hbase.table.lou.column.family",_)).getOrElse(conf.set("hbase.table.lou.column.family","d"))
  Try{config.getString("hbase.table.lou.namespace")}.map(conf.set("hbase.table.lou.namespace",_)).getOrElse(conf.set("hbase.table.lou.namespace",nameSpace))
  Try{config.getString(louHfile)}.map(conf.set(louHfile,_)).getOrElse(conf.set(louHfile,s"$dataDir/lou/hfile"))

  Try{config.getString("files.lou.csv")}.map(conf.set("files.lou.csv",_)).getOrElse(conf.set("files.lou.csv",s"$dataDir/lou.csv"))
  Try{config.getString("files.ent.csv")}.map(conf.set("files.ent.csv",_)).getOrElse(conf.set("files.ent.csv",s"$dataDir/ent.csv"))

  Try{config.getString("enterprise.data.timeperiod")}.map(conf.set("enterprise.data.timeperiod",_)).getOrElse(conf.set("enterprise.data.timeperiod","201801"))
  Try{config.getString("spark.deployment.mode")}.map(conf.set("spark.deployment.mode",_)).getOrElse(conf.set("spark.deployment.mode","local"))

  lazy val HBASE_LINKS_TABLE_NAME = conf.getStrings("hbase.table.links.name").head
  lazy val HBASE_LINKS_TABLE_NAMESPACE = conf.getStrings("hbase.table.links.namespace").head
  lazy val HBASE_LINKS_COLUMN_FAMILY = conf.getStrings("hbase.table.links.column.family").head

  lazy val PATH_TO_LINKS_ENT_HFILE =  conf.getStrings(entLinks).head
  lazy val PATH_TO_LINKS_LOU_HFILE =  conf.getStrings(louLinks).head

  lazy val HBASE_ENTERPRISE_TABLE_NAME = conf.getStrings("hbase.table.enterprise.name").head
  lazy val HBASE_ENTERPRISE_TABLE_NAMESPACE = conf.getStrings("hbase.table.enterprise.namespace").head
  lazy val HBASE_ENTERPRISE_COLUMN_FAMILY = conf.getStrings("hbase.table.enterprise.column.family").head
  lazy val PATH_TO_ENT_HFILE =  conf.getStrings(entHfile).head

  lazy val HBASE_LOU_TABLE_NAME = conf.getStrings("hbase.table.lou.name").head
  lazy val HBASE_LOU_TABLE_NAMESPACE = conf.getStrings("hbase.table.lou.namespace").head
  lazy val HBASE_LOU_COLUMN_FAMILY = conf.getStrings("hbase.table.lou.column.family").head
  lazy val PATH_TO_LOU_HFILE =  conf.getStrings(louHfile).head

  lazy val PATH_TO_LOU_CSV  = conf.getStrings("files.lou.csv").head
  lazy val PATH_TO_ENT_CSV = conf.getStrings("files.ent.csv").head

  lazy val ENTERPRISE_DATA_TIMEPERIOD = conf.getStrings("enterprise.data.timeperiod").head

  def updateConf(args: Array[String]) = {

    Try(args(0)).map(conf.set("hbase.table.links.name", _)).getOrElse(Unit)
    Try(args(1)).map(conf.set("hbase.table.links.namespace", _)).getOrElse(Unit)
    Try(args(2)).map(conf.set(entLinks, _)).getOrElse(Unit)
    Try(args(3)).map(conf.set(louLinks, _)).getOrElse(Unit)

    Try(args(4)).map(conf.set("hbase.table.enterprise.name", _)).getOrElse(Unit)
    Try(args(5)).map(conf.set("hbase.table.enterprise.namespace", _)).getOrElse(Unit)
    Try(args(6)).map(conf.set(entHfile, _)).getOrElse(Unit)

    Try(args(7)).map(conf.set("hbase.table.lou.name", _)).getOrElse(Unit)
    Try(args(8)).map(conf.set("hbase.table.lou.namespace", _)).getOrElse(Unit)
    Try(args(9)).map(conf.set(louHfile, _)).getOrElse(Unit)

    Try(args(10)).map(conf.set("files.lou.csv", _)).getOrElse(Unit)
    Try(args(11)).map(conf.set("files.ent.csv", _)).getOrElse(Unit)

    Try(args(12)).map(conf.set("hbase.zookeeper.quorum", _)).getOrElse(Unit)
    Try(args(13)).map(conf.set("hbase.zookeeper.property.clientPort", _)).getOrElse(Unit)
    Try(args(14)).map(conf.set("enterprise.data.timeperiod", _)).getOrElse(Unit)
  }
}
