/*
 * Copyright (c) 2013.
 * Tinkoff Credit Systems.
 * All rights reserved.
 */

package ru.tcsbank.utils

import java.sql.{Timestamp, Connection, ResultSet}
import scala.collection.{immutable, mutable}


package object database {

  // Simple notion of the connection pool. `Database` needn't to know more.
  trait ConnectionPool {
    def getConnection: Connection
  }

  /*
    SQL string interpolation (forked from https://gist.github.com/mnesarco/4515475)
   */
  case class BoundSql(sql: String, parameters: Seq[_]) {
    // Concatenation of the sql statements
    def +(another: BoundSql) = BoundSql(this.sql + another.sql, this.parameters ++ another.parameters)
    // and the statement with a `String`
    def +(anotherSql: String) = BoundSql(this.sql + anotherSql, this.parameters)
    // concatenates the parameters' sequences
  }

  // Implicit conversion defining custom `String` interpolator sql"select ..."
  implicit class SqlStringContext(val sc: StringContext) extends AnyVal {
    def sql(args : Any*) = BoundSql(sc.parts.mkString("?"), args.toSeq)
  }

  // Out-parameters are also supported, it is possible to call procedures like,
  //
  //    val Seq(output: String) = database execute call (
  //      sql"""begin
  //              our_package.my_procedure('input', ${out(java.sql.Types.VARCHAR)})
  //            end;""")
  //
  // @see tests for an actual usage example.
  //
  case class OutParam(valueType: Int)
  def out(valueType: Int) = OutParam(valueType)

  // Charges database operation (Connection => T) with a powerful method(s)
  implicit class ChargedSqlOperation[T](operation: Connection => T) {

    // Composition on the database operations (having composition itself is not little)
    def ~[T2](anotherOperation: Connection => T2): Connection => T2 = { connection: Connection =>
      operation(connection)
      anotherOperation(connection)
    }
  }


  /*
    ResultSet extractor methods

    Transforming all `ResultSet` records to the single nice Scala object.
    Nicely used with `Database.query`, @see test for examples.
  */
  // Creates native Scala `Option`
  def toOption[V](valueReader: ResultSet => V) = { resultSet: ResultSet =>
    Iterator.continually(resultSet).takeWhile(_.next).find(rs => true).map(valueReader)
  }

  // Creates native Scala `List` from `ResultSet` with list values creating-closure
  def toList[V](valueReader: ResultSet => V) = { resultSet: ResultSet =>
    Iterator.continually(resultSet).takeWhile(_.next).map(valueReader).toList
  }

  // Creates native Scala `Set` from `ResultSet` with set values creating-closure
  def toSet[V](valueReader: ResultSet => V) = { resultSet: ResultSet =>
    Iterator.continually(resultSet).takeWhile(_.next).map(valueReader).toSet
  }

  // Creates native Scala `Map` from `ResultSet` with key and value creating-closures
  def toMap[K,V](keyReader: ResultSet => K, valueReader: ResultSet => V) = { resultSet: ResultSet =>
    Iterator.continually(resultSet).takeWhile(_.next).map(rs => keyReader(rs) -> valueReader(rs)).toMap
  }

  // Creates `Map` with `List` of multiple values from the `ResultSet`
  def toListMultiMap[K,V](keyReader: ResultSet => K, valueReader: ResultSet => V) = { resultSet: ResultSet =>
    val m = mutable.Map.empty[K, mutable.Builder[V,List[V]]]
    for (rs <- Iterator.continually(resultSet).takeWhile(_.next)) {
      val key = keyReader(rs)
      val builder = m.getOrElseUpdate(key, immutable.List.newBuilder[V])
      builder += valueReader(rs)
    }
    val b = immutable.Map.newBuilder[K, List[V]]
    for ((k, v) <- m)
      b += ((k, v.result()))

    b.result()
  }

  // Creates `Map` with `Set` of multiple values from the `ResultSet`
  def toSetMultiMap[K,V](keyReader: ResultSet => K, valueReader: ResultSet => V) = { resultSet: ResultSet =>
    val m = mutable.Map.empty[K, mutable.Builder[V,Set[V]]]
    for (rs <- Iterator.continually(resultSet).takeWhile(_.next)) {
      val key = keyReader(rs)
      val builder = m.getOrElseUpdate(key, immutable.Set.newBuilder[V])
      builder += valueReader(rs)
    }
    val b = immutable.Map.newBuilder[K, Set[V]]
    for ((k, v) <- m)
      b += ((k, v.result()))

    b.result()
  }

  // Creates `Map` with `Map` of multiple key-value pairs from the `ResultSet`
  def toMapMultiMap[K,K2,V](keyReader: ResultSet => K, valueReader: ResultSet => (K2,V)) = { resultSet: ResultSet =>
    val m = mutable.Map.empty[K, mutable.Builder[(K2,V),Map[K2,V]]]
    for (rs <- Iterator.continually(resultSet).takeWhile(_.next)) {
      val key = keyReader(rs)
      val builder = m.getOrElseUpdate(key, immutable.Map.newBuilder[K2,V])
      builder += valueReader(rs)
    }
    val b = immutable.Map.newBuilder[K, Map[K2,V]]
    for ((k, v) <- m)
      b += ((k, v.result()))

    b.result()
  }


  /*
    ResultSet Extensions

    Allows to fetch native Scala types out of the `java.sql.ResultSet`.
   */
  implicit class ExtendedResultSet(resultSet: ResultSet) {

    /*
      Reading id from result set

        val id: Id[Type] = resultSet.getId("id_column")

      It is possible to get Option[Id[T]] whether id existence is not guarantied
      (e.g. when id belongs to the foreign table).
     */
    def getId[T](i: Int): Id[T] = {
      Option(resultSet.getString(i)).map(id => Id[T](id)).getOrElse(null)
    }

    def getId[T](column: String): Id[T] = {
      Option(resultSet.getString(column)).map(id => Id[T](id)).getOrElse(null)
    }

    /*
      Reading `scala.Option` out of the `java.sql.ResultSet`.
     */
    def getOption[T](fetcher: ResultSet => T): Option[T] = {
      val value = fetcher(resultSet)
      if(resultSet.wasNull) None else Option(value)
    }

    def getOptionalId[T](i: Int): Option[Id[T]] = getOption(rs => Id[T](rs.getString(i)))
    def getOptionalId[T](column: String): Option[Id[T]] = getOption(rs => Id[T](rs.getString(column)))

    def getOptionalString(i: Int): Option[String] = getOption(_.getString(i))
    def getOptionalString(column: String): Option[String] = getOption(_.getString(column))

    /*
      Getting numbers

      This methods allow to get options instead of null
      to handle the absence of values properly.
     */
    def getOptionalLong(i: Int): Option[Long] = getOption(_.getLong(i))
    def getOptionalLong(column: String): Option[Long] = getOption(_.getLong(column))

    def getOptionalInt(i: Int): Option[Int] = getOption(_.getInt(i))
    def getOptionalInt(column: String): Option[Int] = getOption(_.getInt(column))

    def getOptionalBigDecimal(i: Int): Option[BigDecimal] = getOption(_.getBigDecimal(i))
    def getOptionalBigDecimal(column: String): Option[BigDecimal] = getOption(_.getBigDecimal(column))

    def getOptionalDouble(i: Int): Option[Double] = getOption(_.getDouble(i))
    def getOptionalDouble(column: String): Option[Double] = getOption(_.getDouble(column))

    /*
      Dealing with time


     */
    def getTime(i: Int): Time = wrapTimestamp(resultSet.getTimestamp(i))
    def getTime(column: String): Time = wrapTimestamp(resultSet.getTimestamp(column))

    def getOptionalTime(column: String) = getOption(rs => wrapTimestamp(rs.getTimestamp(column)))
    def getOptionalTime(i: Int) = getOption(rs => wrapTimestamp(rs.getTimestamp(i)))

    private def wrapTimestamp(timestamp: Timestamp): Time = {
      if(timestamp == null) null
      else Time(timestamp.getTime)
    }

    /* Getting `Class`

       val objectClass: Class[Type] = resultSet.getClass("class_column")
     */
    def getClass[T](i: Int): Class[T] = wrapClass(resultSet.getString(i))
    def getClass[T](column: String): Class[T] = wrapClass(resultSet.getString(column))

    private def wrapClass[T](className: String): Class[T] = {
      if(className == null) null
      else Class.forName(className).asInstanceOf[Class[T]]
    }
  }
}
