package model.domain

import java.util

import scala.util.Try

/**
  *
  */
//case class ReportingUnit(rurn: String, ruref: Option[String],
//                         ern: String, entref: Option[String],
//                         name: String, tradingstyle: Option[String],
//                         legalstatus: Option[String], address1: String,
//                         address2: Option[String], address3: Option[String],
//                         address4: Option[String], address5: Option[String],
//                         postcode: String, sic07: String, employees: String,
//                         employment: String, turnover: String, prn: String)

case class ReportingUnit(rurn: Option[String], ruref: Option[String],
                         ern: Option[String], entref: Option[String],
                         name: Option[String], tradingstyle: Option[String],
                         legalstatus: Option[String], address1: Option[String],
                         address2: Option[String], address3: Option[String],
                         address4: Option[String], address5: Option[String],
                         postcode: Option[String], sic07: Option[String], employees: Option[String],
                         employment: Option[String], turnover: Option[String], prn: Option[String])

object ReportingUnit {

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = new String(row.get(key.getBytes))
    def getOptionValue(key: String) = Try{new String(row.get(key.getBytes))}.toOption

//    new ReportingUnit(
//      getValue("rurn"),
//      getOptionValue("ruref"),
//      getValue("ern"),
//      getOptionValue("entref"),
//      getValue("name"),
//      getOptionValue("trading_style"),
//      getOptionValue("legal_status"),
//      getValue("address1"),
//      getOptionValue("address2"),
//      getOptionValue("address3"),
//      getOptionValue("address4"),
//      getOptionValue("address5"),
//      getValue("postcode"),
//      getValue("sic07"),
//      getValue("employees"),
//      getValue("employment"),
//      getValue("turnover"),
//      getValue("prn")
//    )

    new ReportingUnit(
      getOptionValue("rurn"),
      getOptionValue("ruref"),
      getOptionValue("ern"),
      getOptionValue("entref"),
      getOptionValue("name"),
      getOptionValue("trading_style"),
      getOptionValue("legal_status"),
      getOptionValue("address1"),
      getOptionValue("address2"),
      getOptionValue("address3"),
      getOptionValue("address4"),
      getOptionValue("address5"),
      getOptionValue("postcode"),
      getOptionValue("sic07"),
      getOptionValue("employees"),
      getOptionValue("employment"),
      getOptionValue("turnover"),
      getOptionValue("prn")
    )
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getColumn(field:String) = entry._2.find(_._1==field).get._2
    def getOptionColumn(qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption

//    new ReportingUnit(
    //      getColumn("rurn"),
    //      getOptionColumn("ruref"),
    //      getColumn("ern"),
    //      getOptionColumn("entref"),
    //      getColumn("name"),
    //      getOptionColumn("trading_style"),
    //      getOptionColumn("legal_status"),
    //      getColumn("address1"),
    //      getOptionColumn("address2"),
    //      getOptionColumn("address3"),
    //      getOptionColumn("address4"),
    //      getOptionColumn("address5"),
    //      getColumn("postcode"),
    //      getColumn("sic07"),
    //      getColumn("employees"),
    //      getColumn("employment"),
    //      getColumn("turnover"),
    //      getColumn("prn")
    //    )

    new ReportingUnit(
      getOptionColumn("rurn"),
      getOptionColumn("ruref"),
      getOptionColumn("ern"),
      getOptionColumn("entref"),
      getOptionColumn("name"),
      getOptionColumn("trading_style"),
      getOptionColumn("legal_status"),
      getOptionColumn("address1"),
      getOptionColumn("address2"),
      getOptionColumn("address3"),
      getOptionColumn("address4"),
      getOptionColumn("address5"),
      getOptionColumn("postcode"),
      getOptionColumn("sic07"),
      getOptionColumn("employees"),
      getOptionColumn("employment"),
      getOptionColumn("turnover"),
      getOptionColumn("prn")
    )
  }


}