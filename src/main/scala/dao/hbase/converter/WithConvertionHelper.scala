package dao.hbase.converter


import global.Configs
import model._
import org.apache.spark.sql.Row
import spark.extensions.SQL.SqlRowExtensions
/**
* Schema:
* index | fields
* -------------------
*  0 -   BusinessName: string (nullable = true)
*  1 -   CompanyNo: string (nullable = true)
*  2 -   EmploymentBands: string (nullable = true)
*  3 -   IndustryCode: string (nullable = true)
*  4 -   LegalStatus: string (nullable = true)
*  5 -   PayeRefs: array (nullable = true)
*  6 -   PostCode: string (nullable = true)
*  7 -   TradingStatus: string (nullable = true)
*  8 -   Turnover: string (nullable = true)
*  9 -   UPRN: long (nullable = true)
*  10 -  VatRefs: array (nullable = true)
*  11 -  id: long (nullable = true)
  */
trait WithConvertionHelper {
/*
* Rules:
* fields needed for creating ENTERPRISE UNIT:
* 1. ID(UBRN) - NOT NULL
* ## At least one of the below must be present
* 2. PayeRefs  - NULLABLE
* 3. VatRefs - NULLABLE
* 4. CompanyNo - NULLABLE
* */

  import Configs._

  lazy val period = conf.getStrings("enterprise.data.timeperiod").head


    val legalUnit = "LEU"
    val enterprise = "ENT"

    val childPrefix = "c_"
    val parentPrefix = "p_"

  def toRecords(row:Row): Tables = {
    val ern = getErn(row)
    val idbrref = getIdbrref(row)
    Tables(rowToEnterprise(row,ern,idbrref),rowToLinks(row,ern))
  }


  private def rowToEnterprise(row:Row,ern:String,idbrref:String): Seq[(String, RowObject)] = Seq(createEnterpriseRecord(ern,"ern",ern), createEnterpriseRecord(ern,"idbrref",idbrref))++
        Seq(
          row.getString("name").map(bn  => createEnterpriseRecord(ern,"name",bn)),
          row.getString("postcode").map(pc => createEnterpriseRecord(ern,"postcode",pc)),
          row.getString("status").map(ls => createEnterpriseRecord(ern,"legalstatus",ls))
        ).collect{case Some(v) => v}


  private def rowToLinks(row:Row,ern:String): Seq[(String, RowObject)] = {
      val keyStr = generateLinkKey(ern,enterprise)
      rowToLegalUnitLinks(row,keyStr,ern)
    }

  private def rowToLegalUnitLinks(row:Row, keyStr:String, ern:String):Seq[(String, RowObject)] = row.getStringSeq("ubrn").map(_.flatMap(ubrn => Seq(
    createLinksRecord(keyStr,s"$childPrefix$ubrn",legalUnit),
    createLinksRecord(generateLinkKey(ubrn.toString,legalUnit),s"$parentPrefix$enterprise",ern.toString)
  ))).getOrElse (Seq[(String, RowObject)]())

  private def createLinksRecord(key:String,column:String, value:String) = createRecord(key,HBASE_LINKS_COLUMN_FAMILY,column,value)

  private def createEnterpriseRecord(ern:String,column:String, value:String) = createRecord(generateEntKey(ern),HBASE_ENTERPRISE_COLUMN_FAMILY,column,value)

  private def createRecord(key:String,columnFamily:String, column:String, value:String) = key -> RowObject(key,columnFamily,column,value)

  private def getErn(row:Row) = row.getString("ern").map(_.toString).getOrElse(throw new IllegalArgumentException("ern must be present"))

  private def getIdbrref(row:Row) = row.getString("entref").map(_.toString).getOrElse(throw new IllegalArgumentException("entref must be present"))

  private def generateEntKey(ern:String) = s"${ern.reverse}~$period"

  private def generateLinkKey(id:String, suffix:String) = s"$id~$suffix~$period"



}
