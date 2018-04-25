package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class ReportingUnit(rurn: String, ruref: String, ern: String, entref: String, name: String)

object ReportingUnit {

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = new String(row.get(key.getBytes))

    new ReportingUnit(
      getValue("rurn"),
      getValue("ruref"),
      getValue("ern"),
      getValue("entref"),
      getValue("name"))
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getColumn(field:String) = entry._2.find(_._1==field).get._2

    new ReportingUnit(
      getColumn("rurn"),
      getColumn("ruref"),
      getColumn("ern"),
      getColumn("entref"),
      getColumn("name"))
  }


}