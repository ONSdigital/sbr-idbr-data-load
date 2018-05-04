package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class ReportingUnit(rurn: String, ruref: String, ern: String, entref: String, name: String, tradingstyle: Option[String],
                        address1: String, address2: Option[String], address3: Option[String], address4: Option[String], address5: Option[String],
                        postcode: String, sic07: String, employees: String, employment: String, turnover: String, prn: String)

object ReportingUnit {

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = new String(row.get(key.getBytes))
    def getOptionValue(key: String) = Try{new String(row.get(key.getBytes))}.toOption

    new ReportingUnit(
      getValue("rurn"),
      getValue("ruref"),
      getValue("ern"),
      getValue("entref"),
      getValue("name"),
      getOptionValue("tradingstyle"),
      getValue("address1"),
      getOptionValue("address2"),
      getOptionValue("address3"),
      getOptionValue("address4"),
      getOptionValue("address5"),
      getValue("postcode"),
      getValue("sic07"),
      getValue("employees"),
      getValue("employment"),
      getValue("turnover"),
      getValue("prn")
    )
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getColumn(field:String) = entry._2.find(_._1==field).get._2
    def getOptionColumn(qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption

    new ReportingUnit(
      getColumn("rurn"),
      getColumn("ruref"),
      getColumn("ern"),
      getColumn("entref"),
      getColumn("name"),
      getOptionColumn("tradingstyle"),
      getColumn("address1"),
      getOptionColumn("address2"),
      getOptionColumn("address3"),
      getOptionColumn("address4"),
      getOptionColumn("address5"),
      getColumn("postcode"),
      getColumn("sic07"),
      getColumn("employees"),
      getColumn("employment"),
      getColumn("turnover"),
      getColumn("prn")
    )
  }


}