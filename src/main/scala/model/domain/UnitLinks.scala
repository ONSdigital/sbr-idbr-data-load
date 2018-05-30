package model.domain

import java.util

import scala.util.Try

import model.RowObject

/**
  *
  */
case class UnitLinks(rowkey: String, link: RowObject)

object UnitLinks {

  def apply(row:util.NavigableMap[Array[Byte],Array[Byte]]) = {

    def getValue(key: String) = new String(row.get(key.getBytes))

    new UnitLinks(
      getValue("rowkey"),
      RowObject(s"(${getValue("rowkey")}",getValue("columnFamily"),getValue("qualifer"),getValue("value"))
    )
  }

  def apply(entry:(String, Iterable[(String, String)])) = buildFromHFileDataMap(entry)

  implicit def buildFromHFileDataMap(entry:(String, Iterable[(String, String)])) = {
    def getValue(qualifier:String) = entry._2.find(_._1==qualifier).get._2

    new UnitLinks(
      entry.toString(),
      RowObject("7000000001~ENT~201802","l","c_700000000001","LEU")
      //RowObject(s"${getValue("rowkey")}",getValue("columnFamily"),getValue("qualifer"),getValue("value"))
    )

    //new UnitLinks("7000000001~ENT~201802",RowObject("7000000001~ENT~201802","l","c_700000000001","LEU"))
  }
}