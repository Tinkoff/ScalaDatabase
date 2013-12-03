/*
 * Copyright (c) 2013.
 * Tinkoff Credit Systems.
 * All rights reserved.
 */

package ru.tcsbank.utils.database

import java.sql.{Connection, ResultSet}
import collection.mutable.ListBuffer
import ru.tcsbank.utils._

/*
  Database mock for handy testing of the database operations

  It is supposed to mock `ru.tcsbank.utils.database.Database` in the unit test,
  to facilitate decoupling from the actual database (RDBMS) and mess with JDBC.

  `DatabaseMock` provides means to specify fixed outputs for the inputs and
  to check the ways it was actually invoked, to make unit-tests repeatable.
 */
class DatabaseMock(connectionPool: ConnectionPool) extends Database(connectionPool) {

  // All interactions with the database or supposed outputs are stored here
  private val databaseInvocations = new ListBuffer[DatabaseInvocation[_]]()
  private val databaseOutputs = new ListBuffer[DatabaseOutput[_]]()


  /*
    Database mock preparation

    In order to let DatabaseMock return certain value, `shouldReturn` method should be called in advance.
    Its parameters define what outputs (return values) should be produced by `database` for specified inputs.

    (tip) this method might be chained
  */
  def shouldReturn[T](sqlPieces: Set[String], parameters: Seq[_] = Seq(), output: T) = {
    databaseOutputs.append(DatabaseOutput[T](sqlPieces, parameters, output))
    this
  }

  /*
    Database call verification

    Returns true when database was treated with the specified inputs (sql and parameters).
   */
  def hasExecuted(sqlPieces: Set[String], parameters: Seq[_] = Seq()): Boolean = {
    databaseInvocations exists { invocation =>
      DatabaseOutput[Any](sqlPieces, parameters, None).matches(invocation.sql) && invocation.executed
    }
  }


  /*
    Data structures of the database invocation and the output predefined for the invocation
   */

  private class DatabaseInvocation[T](val sql: BoundSql, var executed: Boolean = false) extends (Connection => T) {

    def apply(connection: Connection): T = {
      this.executed = true

      // Now trying to find suitable output for the specified `sql`
      databaseOutputs.find(_.matches(sql)).map(databaseOutput => extractResult(databaseOutput))
        .getOrElse(throw new IllegalStateException(s"Output is not defined for sql '$sql' and args: ${sql.parameters}!"))
    }

    protected def extractResult[O](databaseOutput: DatabaseOutput[O]): T = {
      databaseOutput.output.asInstanceOf[T]
    }
  }

  private class QueryDatabaseInvocation[T](override val sql: BoundSql,
                                           handler: ResultSet => T) extends DatabaseInvocation[T](sql) {

    // Query must also call `handler` to extract the returning value
    protected override def extractResult[O](databaseOutput: DatabaseOutput[O]): T = {
      handler.apply(databaseOutput.output.asInstanceOf[ResultSet])
    }
  }

  case class DatabaseOutput[T](sqlPieces: Set[String], args: Seq[_] = Seq(), output: T) {

    def matches(sql: BoundSql): Boolean = {
      sqlMatches(sql.sql, sqlPieces) && args == sql.parameters
    }

    // Now simple sanity check, left protected to facilitate more sophisticated extensions (e.g. sql validation)
    protected def sqlMatches(sql: String, sqlPieces: Set[String]): Boolean = {
      val lowerCaseSql = sql.toLowerCase
      sqlPieces.forall(piece => lowerCaseSql.contains(piece.toLowerCase))
    }
  }


  /*
    Implementation of the `ru.tcsbank.utils.database.Database` methods to collect invocations
   */

  override def query[T](sql: BoundSql, handler: ResultSet => T): Connection => T = {
    make(new QueryDatabaseInvocation[T](sql, handler)) { i: DatabaseInvocation[T] =>
      databaseInvocations.append(i)
    }
  }

  override def update(sql: BoundSql): Connection => Int = {
    make(new DatabaseInvocation[Int](sql)) { i: DatabaseInvocation[Int] =>
      databaseInvocations.append(i)
    }
  }

  override def batchUpdate(sqls: Seq[BoundSql]): Connection => Seq[Int] = { connection =>
    make( sqls.map(sql => new DatabaseInvocation[Int](sql)) ) {
      is: Seq[DatabaseInvocation[Int]] => is.foreach(invocation => databaseInvocations.append(invocation))
    } map { i: DatabaseInvocation[Int] => i(connection) }
  }

  override def call(sql: BoundSql): Connection => Seq[AnyRef] = {
    make(new DatabaseInvocation[Seq[AnyRef]](
      sql.copy(parameters = sql.parameters.filter(p => !p.isInstanceOf[OutParam]))
    )) {
      i: DatabaseInvocation[Seq[AnyRef]] => databaseInvocations.append(i)
    }
  }
}