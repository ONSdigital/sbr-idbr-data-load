package dao.hbase.converter


import global.Configs
import model._
import org.apache.spark.sql.Row
import spark.extensions.SQL.SqlRowExtensions

trait WithConvertionHelper {

  import Configs._

  lazy val period = conf.getStrings("enterprise.data.timeperiod").head

    val localUnit = "LOU"
    val enterprise = "ENT"

    val childPrefix = "c_"
    val parentPrefix = "p_"

  def toRecords(row:Row): Tables = {
    val lou = getLou(row)
    val ern = getErn(row)
    val idbrref = getIdbrRef(row)
    val luref = getLuRef(row)
    Tables(rowToLocalUnit(row,lou,luref,ern,idbrref),rowToLinks(row,ern))
  }

  private def rowToLocalUnit(row:Row,lou:String,luref:String,ern:String,idbrref:String): Seq[(String, RowObject)] = Seq(createLocalUnitRecord(ern,lou,"lurn",lou), createLocalUnitRecord(ern,lou,"luref",luref),
    createLocalUnitRecord(ern,lou,"ern",ern), createLocalUnitRecord(ern,lou,"idbrref",idbrref))++
        Seq(
          row.getString("name").map(bn  => createLocalUnitRecord(ern,lou,"name",bn)),
          row.getString("tradstyle").map(pc => createLocalUnitRecord(ern,lou,"tradingstyle",pc)),
          row.getString("address1").map(a1 => createLocalUnitRecord(ern,lou,"address1",a1)),
          row.getString("address2").map(a2 => createLocalUnitRecord(ern,lou,"address2",a2)),
          row.getString("address3").map(a3 => createLocalUnitRecord(ern,lou,"address3",a3)),
          row.getString("address4").map(a4 => createLocalUnitRecord(ern,lou,"address4",a4)),
          row.getString("address5").map(a5 => createLocalUnitRecord(ern,lou,"address5",a5)),
          row.getString("postcode").map(pc => createLocalUnitRecord(ern,lou,"postcode",pc)),
          row.getString("sic07").map(ls => createLocalUnitRecord(ern,lou,"sic07",ls)),
          row.getString("employeees").map(emp => createLocalUnitRecord(ern,lou,"employees",emp))

        ).collect{case Some(v) => v}


  private def rowToLinks(row:Row,ern:String): Seq[(String, RowObject)] = {
      val keyStr = generateLinkKey(ern,enterprise)
      rowToLocalUnitLinks(row,keyStr,ern)
    }

  private def rowToLocalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getStringSeq("lou").map(_.flatMap(lou => Seq(
    createLinksRecord(keyStr,s"$childPrefix$lou",localUnit),
    createLinksRecord(generateLinkKey(lou.toString,localUnit),s"$parentPrefix$enterprise",ern.toString)
  ))).getOrElse (Seq[(String, RowObject)]())

  private def createLinksRecord(key:String,column:String, value:String) = createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)

  private def createLocalUnitRecord(ern:String, lou:String, column:String, value:String) = createRecord(generateLouKey(ern,lou),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> RowObject(key,columnFamily,column,value)

  private def getLou(row:Row) = row.getString("lou").map(_.toString).getOrElse(throw new IllegalArgumentException("lou must be present"))

  private def getLuRef(row:Row) = row.getString("luref").map(_.toString).getOrElse(throw new IllegalArgumentException("luref must be present"))

  private def getErn(row:Row) = row.getString("ern").map(_.toString).getOrElse(throw new IllegalArgumentException("ern must be present"))

  private def getIdbrRef(row:Row) = row.getString("entref").map(_.toString).getOrElse(throw new IllegalArgumentException("entref must be present"))

  private def generateEntKey(ern:String) = s"${ern.reverse}~$period"

  private def generateLouKey(ern:String, lou:String) = s"${ern.reverse}~$period~$lou"

  private def generateLinkKey(id:String, suffix:String) = s"$id~$suffix~$period"



}
