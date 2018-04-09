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
    val legalUnit = "LEU"

    val childPrefix = "c_"
    val parentPrefix = "p_"

  def toLocalUnit(row:Row): Tables = {
    val lurn = getLurn(row)
    val ern = getErn(row)
    val entref = getEntref(row)
    val luref = getLuRef(row)
    val keyStr = generateLinkKey(ern,enterprise)
    Tables(rowToLocalUnit(row,lurn,luref,ern,entref),rowToLocalUnitLinks(row,keyStr,ern))
  }

  def toEnterprise(row:Row): Tables = {
    val ern = getErn(row)
    val entref = getEntref(row)
    val keyStr = generateLinkKey(ern,enterprise)
    Tables(rowToEnterprise(row,ern,entref), rowToLegalUnitLinks(row,keyStr,ern))
  }

  private def rowToLocalUnit(row:Row,lurn:String,luref:String,ern:String,entref:String): Seq[(String, RowObject)] = Seq(createLocalUnitRecord(ern,lurn,"lurn",lurn), createLocalUnitRecord(ern,lurn,"luref",luref),
    createLocalUnitRecord(ern,lurn,"ern",ern), createLocalUnitRecord(ern,lurn,"entref",entref))++
        Seq(
          row.getString("name").map(bn  => createLocalUnitRecord(ern,lurn,"name",bn)),
          row.getString("tradstyle").map(pc => createLocalUnitRecord(ern,lurn,"tradingstyle",pc)),
          row.getString("address1").map(a1 => createLocalUnitRecord(ern,lurn,"address1",a1)),
          row.getString("address2").map(a2 => createLocalUnitRecord(ern,lurn,"address2",a2)),
          row.getString("address3").map(a3 => createLocalUnitRecord(ern,lurn,"address3",a3)),
          row.getString("address4").map(a4 => createLocalUnitRecord(ern,lurn,"address4",a4)),
          row.getString("address5").map(a5 => createLocalUnitRecord(ern,lurn,"address5",a5)),
          row.getString("postcode").map(pc => createLocalUnitRecord(ern,lurn,"postcode",pc)),
          row.getString("sic07").map(ls => createLocalUnitRecord(ern,lurn,"sic07",ls)),
          row.getString("employees").map(emp => createLocalUnitRecord(ern,lurn,"employees",emp))
        ).collect{case Some(v) => v}

  private def rowToEnterprise(row:Row,ern:String,entref:String): Seq[(String, RowObject)] = Seq(createEnterpriseRecord(ern,"ern",ern), createEnterpriseRecord(ern,"entref",entref))++
    Seq(
      row.getString("name").map(bn  => createEnterpriseRecord(ern,"name",bn)),
      row.getString("postcode").map(pc => createEnterpriseRecord(ern,"postcode",pc)),
      row.getString("status").map(ls => createEnterpriseRecord(ern,"legalstatus",ls))
    ).collect{case Some(v) => v}

  private def rowToLocalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getString("lou").map(lou => Seq(
    createLinksRecord(keyStr,s"$childPrefix$lou",localUnit),
    createLinksRecord(generateLinkKey(lou.toString,localUnit),s"$parentPrefix$enterprise",ern.toString)
  )).getOrElse (Seq[(String, RowObject)]())

  private def rowToLegalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getString("ubrn").map(_.flatMap(ubrn => Seq(
    createLinksRecord(keyStr,s"$childPrefix$ubrn",legalUnit),
    createLinksRecord(generateLinkKey(ubrn.toString,legalUnit),s"$parentPrefix$enterprise",ern.toString)
  ))).getOrElse (Seq[(String, RowObject)]())

  private def createLinksRecord(key:String,column:String, value:String) = createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)

  private def createEnterpriseRecord(ern:String,column:String, value:String) = createRecord(generateEntKey(ern),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createLocalUnitRecord(ern:String, lou:String, column:String, value:String) = createRecord(generateLouKey(ern,lou),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> RowObject(key,columnFamily,column,value)

  private def getLurn(row:Row) = row.getString("lou").map(_.toString).getOrElse(throw new IllegalArgumentException("lurn must be present"))

  private def getLuRef(row:Row) = row.getString("luref").map(_.toString).getOrElse(throw new IllegalArgumentException("luref must be present"))

  private def getErn(row:Row) = row.getString("ern").map(_.toString).getOrElse(throw new IllegalArgumentException("ern must be present"))

  private def getEntref(row:Row) = row.getString("entref").map(_.toString).getOrElse(throw new IllegalArgumentException("entref must be present"))

  private def generateEntKey(ern:String) = s"${ern.reverse}~$period"

  private def generateLouKey(ern:String, lou:String) = s"${ern.reverse}~$period~$lou"

  private def generateLinkKey(id:String, suffix:String) = s"$id~$suffix~$period"



}
