package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class LocalUnit(lurn:String, luref:Option[String], ern:String, entref:Option[String],
                     name:String, tradingstyle:Option[String], address1:String, address2:Option[String],
                     address3:Option[String],address4:Option[String],address5:Option[String], postcode:String,
                     sic07:Option[String], employees:Option[String])
object LocalUnit{

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = Try{new String(row.get(key.getBytes))}.toOption
    def getString(key:String) = new String(row.get(key.getBytes))

    new LocalUnit(
      new String(row.get("lurn".getBytes)),
      getValue("luref"),
      getString("ern"),
      getValue("entref"),
      getString("name"),
      getValue("tradingstyle"),
      getString("address1"),
      getValue("address2"),
      getValue("address3"),
      getValue("address4"),
      getValue("address5"),
      getString("postcode"),
      getValue("sic07"),
      getValue("employees"))
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getValue(qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption
    def getString(id: String) = entry._2.find(_._1==id).get._2

    new LocalUnit(
      getString("lurn"),
      getValue("luref"),
      getString("ern"),
      getValue("entref"),
      getString("name"),
      getValue("tradingstyle"),
      getString("address1"),
      getValue("address2"),
      getValue("address3"),
      getValue("address4"),
      getValue("address5"),
      getString("postcode"),
      getValue("sic07"),
      getValue("employees")
    )
  }
}