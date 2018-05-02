package dao.hbase.converter


import global.Configs
import model._
import org.apache.spark.sql.Row
import spark.extensions.SQL.SqlRowExtensions
import Configs._

trait WithConversionHelper {

  val localUnit = "LOU"
  val enterprise = "ENT"
  val legalUnit = "LEU"

  val childPrefix = "c_"
  val parentPrefix = "p_"

  def toRecord(row: Row, recordType: String): Tables = {
    val ern = getID(row, "ern")
    val entref = getID(row, "entref")
    val keyStr = generateLinkKey(ern, enterprise)

    recordType match {
      case "ent" => Tables(rowToEnterprise(row,ern,entref), rowToLegalUnitLinks(row,keyStr,ern))
      case "lou" => {
        val lurn = getID(row, "lou")
        val luref = getID(row, "luref")
        Tables(rowToLocalUnit(row,lurn,luref,ern,entref),rowToLocalUnitLinks(row,keyStr,ern))
      }
    }
  }

  private def rowToLocalUnit(row:Row,lurn:String,luref:String,ern:String,entref:String): Seq[(String, RowObject)] = Seq(createLocalUnitRecord(ern,lurn,"lurn",lurn), createLocalUnitRecord(ern,lurn,"luref",luref), createLocalUnitRecord(ern,lurn,"ern",ern), createLocalUnitRecord(ern,lurn,"entref",entref))++
        Seq(
          row.getString("name").map(bn  => createLocalUnitRecord(ern,lurn,"name",bn.trim)),
          row.getString("tradstyle").map(tradingStyle => createLocalUnitRecord(ern,lurn,"tradingstyle",tradingStyle.trim)),
          row.getString("address1").map(a1 => createLocalUnitRecord(ern,lurn,"address1",a1)),
          row.getString("address2").map(a2 => createLocalUnitRecord(ern,lurn,"address2",a2)),
          row.getString("address3").map(a3 => createLocalUnitRecord(ern,lurn,"address3",a3)),
          row.getString("address4").map(a4 => createLocalUnitRecord(ern,lurn,"address4",a4)),
          row.getString("address5").map(a5 => createLocalUnitRecord(ern,lurn,"address5",a5)),
          row.getString("postcode").map(pc => createLocalUnitRecord(ern,lurn,"postcode",pc)),
          row.getCalcValue("sic07").map(sic => createLocalUnitRecord(ern,lurn,"sic07",sic)),
          row.getCalcValue("employees").map(employees => createLocalUnitRecord(ern,lurn,"employees",employees))
        ).collect{case Some(v) => v}

  private def rowToEnterprise(row:Row,ern:String,entref:String): Seq[(String, RowObject)] = Seq(createEnterpriseRecord(ern,"ern",ern), createEnterpriseRecord(ern,"entref",entref))++
    Seq(
      row.getString("name").map(bn  => createEnterpriseRecord(ern,"name",bn)),
      row.getString("postcode").map(pc => createEnterpriseRecord(ern,"postcode",pc)),
      row.getString("status").map(ls => createEnterpriseRecord(ern,"legalstatus",ls)),
      row.getCalcValue("sic07").map(sic => createEnterpriseRecord(ern,"sic07", sic))
    ).collect{case Some(v) => v}

  private def rowToLocalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getString("lou").map(lou => Seq(
    createLinksRecord(keyStr,s"$childPrefix$lou",localUnit),
    createLinksRecord(generateLinkKey(lou.toString,localUnit),s"$parentPrefix$enterprise",ern.toString)
  )).getOrElse (Seq[(String, RowObject)]())

  private def rowToLegalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getString("ubrn").map(ubrn => Seq(
    createLinksRecord(keyStr,s"$childPrefix$ubrn",legalUnit),
    createLinksRecord(generateLinkKey(ubrn.toString,legalUnit),s"$parentPrefix$enterprise",ern.toString)
  )).getOrElse (Seq[(String, RowObject)]())

  private def createLinksRecord(key:String,column:String, value:String) = createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)

  private def createEnterpriseRecord(ern:String,column:String, value:String) = createRecord(generateEntKey(ern),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createLocalUnitRecord(ern:String, lou:String, column:String, value:String) = createRecord(generateLouKey(ern,lou),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> RowObject(key,columnFamily,column,value)

  private def getID(row:Row,id:String) = row.getString(id).map(_.toString).getOrElse(throw new IllegalArgumentException(s"$id must be present"))

  private def generateEntKey(ern:String) = s"${ern.reverse}~$ENTERPRISE_DATA_TIMEPERIOD"

  private def generateLouKey(ern:String, lou:String) = s"${ern.reverse}~$ENTERPRISE_DATA_TIMEPERIOD~$lou"

  private def generateLinkKey(id:String, suffix:String) = s"$id~$suffix~$ENTERPRISE_DATA_TIMEPERIOD"

}
