package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class ReportingUnit(rurn: String, ruref: Option[String],
                         ern: String, entref: Option[String],
                         name: String, tradingstyle: Option[String],
                         legalstatus: Option[String], address1: String,
                         address2: Option[String], address3: Option[String],
                         address4: Option[String], address5: Option[String],
                         postcode: String, sic07: String, employees: String,
                         employment: String, turnover: String, prn: String)

object ReportingUnit {

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = Try{new String(row.get(key.getBytes))}.toOption
    def getString(key:String) = new String(row.get(key.getBytes))

    new ReportingUnit(
      getString("rurn"),
      getValue("ruref"),
      getString("ern"),
      getValue("entref"),
      getString("name"),
      getValue("trading_style"),
      getValue("legal_status"),
      getString("address1"),
      getValue("address2"),
      getValue("address3"),
      getValue("address4"),
      getValue("address5"),
      getString("postcode"),
      getString("sic07"),
      getString("employees"),
      getString("employment"),
      getString("turnover"),
      getString("prn")
    )
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getValue(qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption
    def getString(id: String) = entry._2.find(_._1==id).get._2

    new ReportingUnit(
      getString("rurn"),
      getValue("ruref"),
      getString("ern"),
      getValue("entref"),
      getString("name"),
      getValue("trading_style"),
      getValue("legal_status"),
      getString("address1"),
      getValue("address2"),
      getValue("address3"),
      getValue("address4"),
      getValue("address5"),
      getString("postcode"),
      getString("sic07"),
      getString("employees"),
      getString("employent"),
      getString("turnover"),
      getString("prn")
    )
  }
}