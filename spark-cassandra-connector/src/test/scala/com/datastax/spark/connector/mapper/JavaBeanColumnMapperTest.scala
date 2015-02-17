package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.ColumnName
import com.datastax.spark.connector.cql.{TableDef, RegularColumn, ColumnDef}
import com.datastax.spark.connector.types.IntType
import org.apache.commons.lang3.SerializationUtils

import org.junit.Assert._
import org.junit.Test
import org.scalatest.{Matchers, FlatSpec}

class JavaBeanColumnMapperTestClass {
  def getProperty1: String = ???
  def setProperty1(str: String): Unit = ???

  def getCamelCaseProperty: Int = ???
  def setCamelCaseProperty(str: Int): Unit = ???

  def isFlagged: Boolean = ???
  def setFlagged(flag: Boolean): Unit = ???
}

class JavaBeanWithWeirdProps {
  def getDevil: Int = ???
  def setDevil(value: Int): Unit = ???

  def getCat: Int = ???
  def setCat(value: Int): Unit = ???

  def getEye: Int = ???
  def setEye(value: Int): Unit = ???
}


object JavaBeanColumnMapperTestClass {
  implicit object Mapper extends JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]()
}

class JavaBeanColumnMapperTest extends FlatSpec with Matchers {

  private val c1 = ColumnDef("test", "table", "property_1", RegularColumn, IntType)
  private val c2 = ColumnDef("test", "table", "camel_case_property", RegularColumn, IntType)
  private val c3 = ColumnDef("test", "table", "flagged", RegularColumn, IntType)
  private val c4 = ColumnDef("test", "table", "marked", RegularColumn, IntType)
  private val c5 = ColumnDef("test", "table", "column", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3, c4, c5))

  it should "testGetters" in  {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    val getters = columnMap.getters
    ColumnName(c1.columnName) shouldBe getters("getProperty1")
    ColumnName(c2.columnName) shouldBe getters("getCamelCaseProperty")
    ColumnName(c3.columnName) shouldBe getters("isFlagged")
  }

  it should "testSetters" in  {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    val setters = columnMap.setters
    ColumnName(c1.columnName) shouldBe setters("setProperty1")
    ColumnName(c2.columnName) shouldBe setters("setCamelCaseProperty")
    ColumnName(c3.columnName) shouldBe setters("setFlagged")
  }

  it should "testColumnNameOverrideGetters" in  {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides).columnMap(tableDef)
    val getters = columnMap.getters
    ColumnName(c5.columnName) shouldBe getters("getProperty1")
    ColumnName(c2.columnName) shouldBe getters("getCamelCaseProperty")
    ColumnName(c4.columnName) shouldBe getters("isFlagged")
  }

  it should "testColumnNameOverrideSetters" in  {
    val columnNameOverrides: Map[String, String] = Map("property1" -> c5.columnName, "flagged" -> c4.columnName)
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass](columnNameOverrides).columnMap(tableDef)
    val setters = columnMap.setters
    ColumnName(c5.columnName) shouldBe setters("setProperty1")
    ColumnName(c2.columnName) shouldBe setters("setCamelCaseProperty")
    ColumnName(c4.columnName) shouldBe setters("setFlagged")
  }

  it should "testSerializeColumnMapper" in  {
    val mapper = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass]
    SerializationUtils.roundtrip(mapper)
  }

  it should "testSerializeColumnMap" in  {
    val columnMap = new JavaBeanColumnMapper[JavaBeanColumnMapperTestClass].columnMap(tableDef)
    SerializationUtils.roundtrip(columnMap)
  }

  it should "testImplicit" in  {
    val mapper = implicitly[ColumnMapper[JavaBeanColumnMapperTestClass]]
    assertTrue(mapper.isInstanceOf[JavaBeanColumnMapper[_]])
  }

  it should "work with aliases" in {
    val mapper = new JavaBeanColumnMapper[ClassWithWeirdProps]()
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c5.columnName))
    expectedConstructor shouldBe map.constructor
  }

  it should "work with aliases and honor overrides" in {
    val mapper = new JavaBeanColumnMapper[ClassWithWeirdProps](Map("cat" -> "marked"))
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c4.columnName),
      ColumnName(c5.columnName))
    expectedConstructor shouldBe map.constructor
  }

}
