package com.datastax.spark.connector.writer

import com.datastax.spark.connector.cql.TableDef
import com.datastax.spark.connector.mapper.ColumnMapper
import com.datastax.spark.connector.{ColumnIndex, ColumnName, ColumnRef}

import scala.collection.JavaConversions._
import scala.collection.Seq

trait CheckSetting
object CheckLevel{
  case object CheckAll extends CheckSetting
  case object CheckPartitionOnly extends CheckSetting
}

/** A `RowWriter` suitable for saving objects mappable by a [[com.datastax.spark.connector.mapper.ColumnMapper ColumnMapper]].
  * Can save case class objects, java beans and tuples. */
class DefaultRowWriter[T: ColumnMapper](table: TableDef, selectedColumns: Seq[String])
  extends RowWriter[T] {

  private val columnMapper = implicitly[ColumnMapper[T]]
  private val cls = columnMapper.classTag.runtimeClass.asInstanceOf[Class[T]]
  private val columnMap = columnMapper.columnMap(table)
  private val selectedColumnsSet = selectedColumns.toSet
  private val selectedColumnsIndexed = selectedColumns.toIndexedSeq

  private def checkMissingProperties(requestedPropertyNames: Seq[String]) {
    val availablePropertyNames = PropertyExtractor.availablePropertyNames(cls, requestedPropertyNames)
    val missingColumns = requestedPropertyNames.toSet -- availablePropertyNames
    if (missingColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"One or more properties not found in RDD data: ${missingColumns.mkString(", ")}")
  }

  private def checkUndefinedColumns(mappedColumns: Seq[String]) {
    val undefinedColumns = selectedColumns.toSet -- mappedColumns.toSet
    if (undefinedColumns.nonEmpty)
      throw new IllegalArgumentException(
        s"Missing required columns in RDD data: ${undefinedColumns.mkString(", ")}"
      )
  }

  private def columnNameByRef(columnRef: ColumnRef): Option[String] = {
    columnRef match {
      case ColumnName(name) if selectedColumnsSet.contains(name) => Some(name)
      case ColumnIndex(index) if index < selectedColumns.size => Some(selectedColumnsIndexed(index))
      case _ => None
    }
  }

  val (propertyNames, columnNames) = {
    val propertyToColumnName = columnMap.getters.mapValues(columnNameByRef).toSeq
    val selectedPropertyColumnPairs =
      for ((propertyName, Some(columnName)) <- propertyToColumnName if selectedColumnsSet.contains(columnName))
      yield (propertyName, columnName)
    selectedPropertyColumnPairs.unzip
  }

  checkMissingProperties(propertyNames)
  checkUndefinedColumns(columnNames)

  private val columnNameToPropertyName = (columnNames zip propertyNames).toMap
  private val extractor = new PropertyExtractor(cls, propertyNames)

  override def readColumnValues(data: T, buffer: Array[Any]) = {
    for ((c, i) <- columnNames.zipWithIndex) {
      val propertyName = columnNameToPropertyName(c)
      val value = extractor.extractProperty(data, propertyName)
      buffer(i) = value
    }
  }
}

object DefaultRowWriter {

  def factory[T : ColumnMapper] = new RowWriterFactory[T] {
    override def rowWriter(tableDef: TableDef, columnNames: Seq[String]): RowWriter[T] = {
      new DefaultRowWriter[T](tableDef, columnNames)
    }
  }
}

