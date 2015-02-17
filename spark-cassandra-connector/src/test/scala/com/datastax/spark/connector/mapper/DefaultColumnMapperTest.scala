package com.datastax.spark.connector.mapper

import com.datastax.spark.connector.{NamedColumnRef, ColumnName}
import com.datastax.spark.connector.cql.{RegularColumn, TableDef, ColumnDef}
import com.datastax.spark.connector.types.IntType
import org.apache.commons.lang3.SerializationUtils
import org.scalatest.{Matchers, FlatSpec}

case class DefaultColumnMapperTestClass1(property1: String, camelCaseProperty: Int, UpperCaseColumn: Int)

class DefaultColumnMapperTestClass2(var property1: String, var camelCaseProperty: Int, var UpperCaseColumn: Int)

case class ClassWithWeirdProps(devil: String, cat: Int, eye: Double)

class DefaultColumnMapperTest extends FlatSpec with Matchers {

  private val c1 = ColumnDef("test", "table", "property_1", RegularColumn, IntType)
  private val c2 = ColumnDef("test", "table", "camel_case_property", RegularColumn, IntType)
  private val c3 = ColumnDef("test", "table", "UpperCaseColumn", RegularColumn, IntType)
  private val c4 = ColumnDef("test", "table", "column", RegularColumn, IntType)
  private val tableDef = TableDef("test", "table", Seq(c1), Seq(c2), Seq(c3, c4))

  it should "testGetters1" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    val getters = columnMap.getters
    ColumnName(c1.columnName) shouldBe getters("property1")
    ColumnName(c2.columnName) shouldBe getters("camelCaseProperty")
    ColumnName(c3.columnName) shouldBe getters("UpperCaseColumn")
  }

  it should "testGetters2" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val getters = columnMap.getters
    ColumnName(c1.columnName) shouldBe getters("property1")
    ColumnName(c2.columnName) shouldBe getters("camelCaseProperty")
    ColumnName(c3.columnName) shouldBe getters("UpperCaseColumn")
  }

  it should "testSetters1" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    columnMap.setters.isEmpty shouldBe true
  }

  it should "testSetters2" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val setters = columnMap.setters
    ColumnName(c1.columnName) shouldBe setters("property1_$eq")
    ColumnName(c2.columnName) shouldBe setters("camelCaseProperty_$eq")
    ColumnName(c3.columnName) shouldBe setters("UpperCaseColumn_$eq")
  }

  it should "testConstructorParams1" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1].columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    expectedConstructor shouldBe columnMap.constructor
  }

  it should "testConstructorParams2" in  {
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2].columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    expectedConstructor shouldBe columnMap.constructor
  }

  it should "columnNameOverrideGetters" in  {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride).columnMap(tableDef)
    val getters = columnMap.getters
    ColumnName(c4.columnName) shouldBe getters("property1")
    ColumnName(c2.columnName) shouldBe getters("camelCaseProperty")
    ColumnName(c3.columnName) shouldBe getters("UpperCaseColumn")
  }

  it should "columnNameOverrideSetters" in  {
    val nameOverride: Map[String, String] = Map("property1" -> c4.columnName)
    val columnMap = new DefaultColumnMapper[DefaultColumnMapperTestClass2](nameOverride).columnMap(tableDef)
    val setters = columnMap.setters
    ColumnName(c4.columnName) shouldBe setters("property1_$eq")
    ColumnName(c2.columnName) shouldBe setters("camelCaseProperty_$eq")
    ColumnName(c3.columnName) shouldBe setters("UpperCaseColumn_$eq")
  }

  it should "columnNameOverrideConstructor" in  {
    val nameOverride: Map[String, String] = Map("property1" -> "column")
    val mapper = new DefaultColumnMapper[DefaultColumnMapperTestClass1](nameOverride).columnMap(tableDef)
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c4.columnName),
      ColumnName(c2.columnName),
      ColumnName(c3.columnName))
    expectedConstructor shouldBe mapper.constructor
  }

  it should "testSerialize" in  {
    val mapper = new DefaultColumnMapper[DefaultColumnMapperTestClass1]
    SerializationUtils.roundtrip(mapper)
  }

  it should "testImplicit" in  {
    val mapper = implicitly[ColumnMapper[DefaultColumnMapperTestClass1]]
    mapper shouldBe a[DefaultColumnMapper[_]]
  }

  it should "work with aliases" in {
    val mapper = new DefaultColumnMapper[ClassWithWeirdProps]()
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c2.columnName),
      ColumnName(c4.columnName))
    expectedConstructor shouldBe map.constructor
  }

  it should "work with aliases and honor overrides" in {
    val mapper = new JavaBeanColumnMapper[ClassWithWeirdProps](Map("cat" -> "UpperCaseColumn"))
    val map = mapper.columnMap(tableDef, Map("devil" -> "property_1", "cat" -> "camel_case_property", "eye" -> "column"))
    val expectedConstructor: Seq[ColumnName] = Seq(
      ColumnName(c1.columnName),
      ColumnName(c3.columnName),
      ColumnName(c4.columnName))
    expectedConstructor shouldBe map.constructor
  }

}
