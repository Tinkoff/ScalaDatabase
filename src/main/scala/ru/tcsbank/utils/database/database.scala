/*
 * Copyright (c) 2013.
 * Tinkoff Credit Systems.
 * All rights reserved.
 */

package ru.tcsbank.utils.database

import java.sql._
import ru.tcsbank.utils._
import org.apache.commons.dbutils.{QueryRunner, ResultSetHandler}


class Database(pool: ConnectionPool) {
  private val queryRunner = new QueryRunner()

  /*
    Execution on the database queries

    Method `execute` executes database operation on `java.sql.Connection`.
    The most useful ones are predefined below, including,

     * 'query' — for SELECT queries
     * 'update' — for UPDATE, INSERT, DELETE, MERGE sql statements
     * 'batchUpdate' — for execution of UPDATE statement with multiple sets of arguments
     * 'call' — for execution of the stored procedures/functions as well as pl/sql code
     * any composition of these or custom function on `java.sql.Connection`

    `execute` is responsible for drawing connection out of the `ConnectionPool` and putting it back.
    For correct operations composition disables "auto commit" for the connection and commits manually.
    Performs `connection.rollback` on `java.sql.SQLException`.

    Returns result of the operation application.
   */
  def execute[T](operation: Connection => T): T = {
    using(make(pool.getConnection) { c: Connection => c.setAutoCommit(false) }) { connection =>
      try {
        val result = operation(connection)
        connection.commit()
        result
      } catch {
        case e: SQLException => {
          connection.rollback()
          throw e
        }
      }
    }
  }

  /*
    Query

    Provides SELECT queries to the database.

    Sql queries are implemented with the `BoundSql` which constitutes of
    the query string and the sequence of bind-parameters.
    Bind-parameters are matched with sql query by custom string interpolator `sql`.

    You might use one of the extractors from package object to produce the container you need:

      - `toOption` for `Option[T]`
      - `toList` for `List[T]`
      - `toSet` for `Set[T]`
      - `toMap` for `toMap[K, V]`
      - `toListMultiMap` for `Map[K, List[V]]`
      - `toSetMultiMap` for `Map[K, Set[V]]`
      - `toMapMultiMap` for `Map[K1, Map[K2, V]]`

    Inside of the extractor you may rely on the methods from `ExtendedResultSet`
    extracting native scala from `java.sql.ResultSet`.

    Returns function from `java.sql.Connection` to be passed to `database.execute`
    to retrieve the query result.
   */
  def query[T](sql: BoundSql, handler: ResultSet => T): Connection => T = { connection =>
    queryRunner.query(connection, sql.sql,
      new ResultSetHandler[T] {
        def handle(rs: ResultSet) = handler(rs)
      },
      castInputParameters(sql.parameters, connection): _*)
  }

  /*
    Update

    Provides execution of UPDATE, INSERT, DELETE, MERGE sql statements.

    Sql statements are implemented with the `BoundSql` which constitutes of
    the query string and the sequence of bind-parameters.
    Bind-parameters are matched with sql query by custom string interpolator `sql`.

    Returns function from `java.sql.Connection` to be passed to `database.execute`
    to retrieve the count of the records updated.
   */
  def update(sql: BoundSql): Connection => Int = { connection =>
    queryRunner.update(connection: Connection, sql.sql,
      castInputParameters(sql.parameters, connection): _*)
  }

  /*
    Batch update

    Provides execution of UPDATE statement with multiple sets of arguments at-once.

    Sql statements are implemented with the `BoundSql` which constitutes of
    the query string and the sequence of bind-parameters.
    Bind-parameters are matched with sql query by custom string interpolator `sql`.

    Returns function from `java.sql.Connection` to be passed to `database.execute`
    to retrieve the count of the records updated for each of the sql statement from `sqls`.
   */
  def batchUpdate(sqls: Seq[BoundSql]): Connection => Seq[Int] = { connection =>
    if(sqls.isEmpty) Seq()
    else queryRunner.batch(connection: Connection, sqls.head.sql,
           sqls.map(sql => castInputParameters(sql.parameters, connection)).toArray)
  }

  /*
    Call

    Provides execution of the stored procedures and functions as well as pl/sql code fragments.
    Supports out-parameters binding with `OutParam`, @see test case for example.

    Sql statements are implemented with the `BoundSql` which constitutes of
    the query string and the sequence of input and output bind-parameters.
    Bind-parameters are matched with sql query by custom string interpolator `sql`.

    Returns function from `java.sql.Connection` to be passed to `database.execute`
    to retrieve sequence of values for the bound out-parameters.
   */
  def call(sql: BoundSql): Connection => Seq[Any] = { connection =>
    using(connection.prepareCall(sql.sql)) { callableStatement =>

      val indexedParams = sql.parameters.zipWithIndex

      indexedParams foreach {
        case (OutParam(valueType), index) => callableStatement.registerOutParameter(index + 1, valueType)
        case (value, index) => registerParameter(callableStatement, index + 1, castInputParameter(value, connection))
      }

      callableStatement.execute

      indexedParams collect {
        case (_: OutParam, index) => callableStatement.getObject(index + 1)
      }
    }
  }

  // Transform Scala sequence of parameters to the array comprehensible for `org.apache.commons.dbutils`
  private def castInputParameters (parameters : Seq[_], connection: Connection): scala.Array[AnyRef] = {
    parameters.map(castInputParameter(_, connection).asInstanceOf[AnyRef]).toArray
  }

  // Transform native Scala types to the types comprehensible for JDBC
  private def castInputParameter(parameter: Any, connection: Connection): Any = {
    parameter match {
      case id: Id[_]                   => id.value
      case time: ru.tcsbank.utils.Time => new Timestamp(time.milliseconds)
      case Some(value)                 => castInputParameter(value, connection)
      case None                        => null
      case _                           => parameter
    }
  }

  // Fills input parameters to the `PreparedStatement` in a type-specific manner
  private def registerParameter(ps: PreparedStatement, index: Int, value: Any) {
    value match {
      case string: String             => ps.setString(index, string)
      case integer: java.lang.Integer => ps.setInt(index, integer)
      case longValue: java.lang.Long  => ps.setLong(index, longValue)
      case _                          => ps.setObject(index, value)
    }
  }
}

