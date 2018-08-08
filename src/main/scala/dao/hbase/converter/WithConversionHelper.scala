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
  val reportingUnit = "REU"

  val childPrefix = "c_"
  val parentPrefix = "p_"

  def toRecord(row: Row, recordType: String): Tables = {
    val ern = getID(row, "ern")
    val entref = getID(row, "entref")
    val keyStr = generateLinkKey(ern, enterprise)

    recordType match {
      case "ent" => Tables(rowToEnterprise(row, ern, entref), rowToLegalUnitLinks(row, keyStr, ern, "ubrns", legalUnit, enterprise))
      case "lou" => {
        val lurn  = getID(row, "lou")
        val luref = getID(row, "luref")
        //val rurn  = getID(row, "rurn")
        Tables(rowToLocalUnit(row, lurn, luref ,ern, entref), rowToLocalUnitLinks(row, keyStr, ern/*, rurn*/))
      }
      case "reu" => {
        val rurn  = getID(row, "rurn")
        val ruref = getID(row, "ruref")
        Tables(rowToReportingUnit(row, rurn, ruref, ern, entref), rowToUnitLinks(row, keyStr, ern, "rurn", reportingUnit, enterprise))
      }
    }
  }

  private def rowToLocalUnit(row: Row, lurn: String, luref: String,ern:String, entref: String): Seq[(String, RowObject)] = Seq(createUnitRecord(ern, lurn, "lurn", lurn), createUnitRecord(ern, lurn, "luref", luref), createUnitRecord(ern, lurn, "ern", ern), createUnitRecord(ern, lurn, "entref", entref))++
    Seq(
      //row.getString("rurn").map(rurn => createUnitRecord(ern, lurn, "rurn", rurn)),
      //row.getString("ruref").map(ruref => createUnitRecord(ern, lurn, "ruref", ruref)),
      row.getString("name").map(bn  => createUnitRecord(ern,lurn,"name",bn.trim)),
      row.getString("tradstyle").map(tradingStyle => createUnitRecord(ern,lurn,"trading_style",tradingStyle.trim)),
      row.getString("address1").map(a1 => createUnitRecord(ern,lurn,"address1",a1)),
      row.getString("address2").map(a2 => createUnitRecord(ern,lurn,"address2",a2)),
      row.getString("address3").map(a3 => createUnitRecord(ern,lurn,"address3",a3)),
      row.getString("address4").map(a4 => createUnitRecord(ern,lurn,"address4",a4)),
      row.getString("address5").map(a5 => createUnitRecord(ern,lurn,"address5",a5)),
      row.getString("postcode").map(pc => createUnitRecord(ern,lurn,"postcode",pc)),
      row.getCalcValue("sic07").map(sic => createUnitRecord(ern,lurn,"sic07",sic)),
      row.getCalcValue("employees").map(employees => createUnitRecord(ern,lurn,"employees",employees))
    ).collect{case Some(v) => v}

  private def rowToEnterprise(row: Row, ern: String, entref: String): Seq[(String, RowObject)] = Seq(createEnterpriseRecord(ern, "ern", ern), createEnterpriseRecord(ern, "entref", entref))++
    Seq(
      row.getString("name").map(bn  => createEnterpriseRecord(ern,"name",bn)),
      row.getString("tradstyle").map(tradingStyle => createEnterpriseRecord(ern,"trading_style",tradingStyle.trim)),
      row.getString("address1").map(a1 => createEnterpriseRecord(ern,"address1",a1)),
      row.getString("address2").map(a2 => createEnterpriseRecord(ern,"address2",a2)),
      row.getString("address3").map(a3 => createEnterpriseRecord(ern,"address3",a3)),
      row.getString("address4").map(a4 => createEnterpriseRecord(ern,"address4",a4)),
      row.getString("address5").map(a5 => createEnterpriseRecord(ern,"address5",a5)),
      row.getString("postcode").map(pc => createEnterpriseRecord(ern,"postcode",pc)),
      row.getString("status").map(ls => createEnterpriseRecord(ern,"legal_status",ls)),
      row.getCalcValue("sic").map(sic => createEnterpriseRecord(ern,"sic07", sic))
    ).collect{case Some(v) => v}

  private def rowToReportingUnit(row: Row, rurn: String, ruref: String, ern: String, entref: String): Seq[(String, RowObject)] = Seq(createUnitRecord(ern, rurn, "rurn", rurn), createUnitRecord(ern, rurn, "ruref", ruref), createUnitRecord(ern, rurn, "entref", entref), createUnitRecord(ern, rurn, "ern", ern)) ++
    Seq(
      row.getString("name").map(bn  => createUnitRecord(ern, rurn, "name", bn)),
      row.getString("tradestyle").map(tradingStyle => createUnitRecord(ern, rurn, "trading_style", tradingStyle.trim)),
      row.getString("legalstatus").map(ls => createUnitRecord(ern, rurn, "legal_status", ls)),
      row.getString("address1").map(a1 => createUnitRecord(ern, rurn, "address1", a1)),
      row.getString("address2").map(a2 => createUnitRecord(ern, rurn, "address2", a2)),
      row.getString("address3").map(a3 => createUnitRecord(ern, rurn, "address3", a3)),
      row.getString("address4").map(a4 => createUnitRecord(ern, rurn, "address4", a4)),
      row.getString("address5").map(a5 => createUnitRecord(ern, rurn, "address5", a5)),
      row.getString("postcode").map(pc => createUnitRecord(ern, rurn, "postcode", pc)),
      row.getString("rusic07").map(sic => createUnitRecord(ern, rurn, "sic07", sic)),
      row.getString("employees").map(employees => createUnitRecord(ern, rurn, "employees", employees)),
      row.getString("employment").map(employment => createUnitRecord(ern, rurn, "employment", employment)),
      row.getString("turnover").map(turnover => createUnitRecord(ern, rurn, "turnover", turnover)),
      row.getString("prn").map(prn => createUnitRecord(ern, rurn, "prn", prn))
    ).collect{case  Some(v) => v}

  private def rowToLocalUnitLinks(row: Row, keyStr: String, ern: String/*, rurn: String*/):Seq[(String, RowObject)] = row.getString("lou").map(lou => Seq(
    createLinksRecord(keyStr, s"$childPrefix$lou", localUnit),
    //createLinksRecord(generateLinkKey(rurn, reportingUnit), s"$childPrefix$lou", localUnit),
    createLinksRecord(generateLinkKey(lou, localUnit), s"$parentPrefix$enterprise", ern)//,
    //createLinksRecord(generateLinkKey(lou, localUnit), s"$parentPrefix$reportingUnit", rurn)
  )).getOrElse (Seq[(String, RowObject)]())

  private def rowToUnitLinks(row:Row, keyStr:String, ern:String, unitType: String, childType: String, parentType: String):Seq[(String, RowObject)] = row.getString(unitType).map(unitType => Seq(
    createLinksRecord(keyStr,s"$childPrefix$unitType",childType),
    createLinksRecord(generateLinkKey(unitType,childType),s"$parentPrefix$parentType",ern)
  )).getOrElse (Seq[(String, RowObject)]())

  private def rowToLegalUnitLinks(row:Row, keyStr:String, ern:String, unitType: String, childType: String, parentType: String):Seq[(String, RowObject)] = row.getStringSeq(unitType).map(_.flatMap(unitType => Seq(
    createLinksRecord(keyStr,s"$childPrefix$unitType",childType),
    createLinksRecord(generateLinkKey(unitType,childType),s"$parentPrefix$parentType",ern)
  ))).getOrElse (Seq[(String, RowObject)]())

  private def createLinksRecord(key: String, column: String, value: String) = createRecord(key ,HBASE_LINKS_COLUMN_FAMILY, column, value)

  private def createEnterpriseRecord(ern: String,column: String, value: String) = createRecord(generateEntKey(ern), HBASE_COLUMN_FAMILY, column, value)

  private def createUnitRecord(ern: String, unitRef: String, column: String, value: String) = createRecord(generateKey(ern, unitRef), HBASE_COLUMN_FAMILY, column, value)

  private def createRecord(key: String, columnFamily: String, column: String, value: String) = key -> RowObject(key, columnFamily, column, value)

  private def getID(row: Row, id: String) = row.getString(id).map(_.toString).getOrElse(throw new IllegalArgumentException(s"$id must be present"))

  private def generateEntKey(id:String) = s"${id.reverse}"

  private def generateKey(ern:String, lou:String) = s"${ern.reverse}~$lou"

  private def generateLinkKey(id:String, suffix:String) = s"$suffix~$id"
}
