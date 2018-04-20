package model.domain

import java.util

import scala.util.Try

/**
  *
  */
case class Enterprise(ern:String, entref:Option[String], businessName:Option[String], PostCode:Option[String], legalStatus:Option[String], sic:Option[String])

object Enterprise{

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key:String) = Try{new String(row.get(key.getBytes))}.toOption

    new Enterprise(
      new String(row.get("ern".getBytes)),
      getValue("entref"),
      getValue("name"),
      getValue("postcode"),
      getValue("legalstatus"),
      getValue("sic07"))
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {

    def getValue(qualifier:String) = Try{entry._2.find(_._1==qualifier).get._2}.toOption
    val ern = entry._2.find(_._1=="ern").get._2

    new Enterprise(
      ern,
      getValue("entref"),
      getValue("name"),
      getValue("postcode"),
      getValue("legalstatus"),
      getValue("sic07"))
  }


}