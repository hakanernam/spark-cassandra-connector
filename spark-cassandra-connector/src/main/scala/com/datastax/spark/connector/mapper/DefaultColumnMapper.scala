package com.datastax.spark.connector.mapper

import java.lang.reflect.Method

import com.datastax.spark.connector.cql.TableDef

import scala.reflect.ClassTag

/** A [[ColumnMapper]] that assumes camel case naming convention for property accessors and constructor names
  * and underscore naming convention for column names.
  *
  * Example mapping:
  * {{{
  *   case class User(
  *     login: String,         // mapped to "login" column
  *     emailAddress: String   // mapped to "email_address" column
  *     emailAddress2: String  // mapped to "email_address_2" column
  *   )
  * }}}
  *
  * Additionally, it is possible to name columns exactly the same as property names (case-sensitive):
  * {{{
  *   case class TaxPayer(
  *     TIN: String            // mapped to "TIN" column
  *   )
  * }}}
  *
  * @param columnNameOverride maps property names to column names; use it to override default mapping for some properties
  */
class DefaultColumnMapper[T : ClassTag](columnNameOverride: Map[String, String] = Map.empty) extends ReflectionColumnMapper[T] {

  import com.datastax.spark.connector.mapper.DefaultColumnMapper._

  override def classTag: ClassTag[T] = implicitly[ClassTag[T]]

  private def setterNameToPropertyName(str: String) =
    str.substring(0, str.length - SetterSuffix.length)

  override def isGetter(method: Method) = {
    method.getParameterTypes.size == 0 &&
    method.getReturnType != Void.TYPE
  }

  override def isSetter(method: Method) = {
    method.getParameterTypes.size == 1 &&
    method.getReturnType == Void.TYPE &&
    method.getName.endsWith(SetterSuffix)
  }

  def resolve(name: String, tableDef: TableDef, aliasToColumnName: Map[String, String]): String = {
    columnNameOverride orElse aliasToColumnName applyOrElse(name, columnNameForProperty(_: String, tableDef))
  }

  override def constructorParamToColumnName(paramName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]) =
    resolve(paramName, tableDef, aliasToColumnName)

  override def getterToColumnName(getterName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]) =
    resolve(getterName, tableDef, aliasToColumnName)

  override def setterToColumnName(setterName: String, tableDef: TableDef, aliasToColumnName: Map[String, String]) = {
    val propertyName = setterNameToPropertyName(setterName)
    resolve(propertyName, tableDef, aliasToColumnName)
  }

  /** Don't allow nulls in Scala - fail fast with NPE if null is tried. */
  override protected def allowsNull = false
}

object DefaultColumnMapper {
  private val SetterSuffix: String = "_$eq"
}
