package com.datastax.spark.connector.rdd

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, SparkContext}

import scala.language.existentials
import scala.reflect.ClassTag

abstract class CassandraRDD[R](_sc: SparkContext, dep: Seq[Dependency[_]])(implicit ct: ClassTag[R]) extends RDD[R](_sc, dep) {

  /** Allows to set custom read configuration, e.g. consistency level or fetch size. */
  def withReadConf(readConf: ReadConf) = {
    copy(readConf = readConf)
  }

  protected def columnNames: ColumnSelector

  protected def where: CqlWhereClause

  protected def readConf: ReadConf

  protected def connector: CassandraConnector

  protected def copy(columnNames: ColumnSelector = columnNames,
                     where: CqlWhereClause = where,
                     readConf: ReadConf = readConf, connector: CassandraConnector = connector): CassandraRDD[R]

  /** Returns a copy of this Cassandra RDD with specified connector */
  def withConnector(connector: CassandraConnector): CassandraRDD[R] = {
    copy(connector = connector)
  }

  /** Adds a CQL `WHERE` predicate(s) to the query.
    * Useful for leveraging secondary indexes in Cassandra.
    * Implicitly adds an `ALLOW FILTERING` clause to the WHERE clause, however beware that some predicates
    * might be rejected by Cassandra, particularly in cases when they filter on an unindexed, non-clustering column. */
  def where(cql: String, values: Any*): CassandraRDD[R] = {
    copy(where = where and CqlWhereClause(Seq(cql), values))
  }

  /** Narrows down the selected set of columns.
    * Use this for better performance, when you don't need all the columns in the result RDD.
    * When called multiple times, it selects the subset of the already selected columns, so
    * after a column was removed by the previous `select` call, it is not possible to
    * add it back.
    *
    * The selected columns are [[NamedColumnRef]] instances. This type allows to specify columns for
    * straightforward retrieval and to read TTL or write time of regular columns as well. Implicit
    * conversions included in [[com.datastax.spark.connector]] package make it possible to provide
    * just column names (which is also backward compatible) and optional add `.ttl` or `.writeTime`
    * suffix in order to create an appropriate [[NamedColumnRef]] instance.
    */
  def select(columns: NamedColumnRef*): CassandraRDD[R] = {
    copy(columnNames = SomeColumns(narrowColumnSelection(columns): _*))
  }

  def narrowColumnSelection(columns: Seq[NamedColumnRef]): Seq[NamedColumnRef] //TODO extract this out to READER Trait
}


