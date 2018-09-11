package spark.extensions

import org.apache.spark.sql.Row

/**
  *
  */
package object SQL {

  implicit class SqlRowExtensions(val row:Row) {

    def getString(field: String): Option[String] = getValue[String](field)

    def getLong(field :String): Option[Long] = getValue[Long](field)

    def getInt(field: String): Option[Int] = getValue[Int](field)

    def getStringSeq(field: String): Option[Seq[String]] = getSeq(field,Some((s:String) => s.trim.nonEmpty))

    def getLongSeq(field: String): Option[Seq[Long]] = getSeq[Long](field)

    def getSeq[T](fieldName: String, eval:Option[T => Boolean] = None): Option[Seq[T]] = if(isNull(fieldName)) None else Some(row.getSeq[T](row.fieldIndex(fieldName)).filter(v => v!=null && eval.map(_(v)).getOrElse(true)))

    def isNull(field :String) = row.isNullAt(row.fieldIndex(field))

    def getCalcValue(fieldName: String): Option[String] = {
      val v = isNull(fieldName)
      v match{
        case true  => Some("")
        case false => Some(row.getAs(fieldName).toString)
      }
    }

    def getValue[T](
                   fieldName:String,
                   eval:Option[T => Boolean] = None
                 ): Option[T] = if(isNull(fieldName)) None else {

      val v = row.getAs[T](fieldName)
      if (v.isInstanceOf[String] && v.asInstanceOf[String].trim.isEmpty) None
      else eval match{
        case Some(f) => if(f(v)) Some(v) else None
        case None  => Some(v)
      }}

    /**
      * returns option of value
      * Retuns None if field is present but value is null or if field is not present
      * returns Some(VALUE_OF_THE_FIELD) otherwise
      * */
    def getOption[T](field:String)= {
      if(row.isNull(field)) None
      else Option[T](row.getAs[T](field))
    }

    /**
      * Returns None if:
      * 1. row does not contain field with given name
      * 2. value is null
      * 3. value's data type is not String
      * returns Some(...) of value
      * otherwise
      * */
    def getStringOption(name:String) = {
      getOption[String](name)
    }

    def getValueOrEmptyStr(fieldName:String) = getStringOption(fieldName).getOrElse("")

    def getValueOrNull(fieldName:String) = getStringOption(fieldName).getOrElse(null)

   }
}
