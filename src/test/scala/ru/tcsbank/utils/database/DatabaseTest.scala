/*
 * Copyright (c) 2013.
 * Tinkoff Credit Systems.
 * All rights reserved.
 */

package ru.tcsbank.utils.database

import java.sql._
import org.scalatest._
import org.mockito.Mockito._
import org.mockito.Mockito
import ru.tcsbank.utils.make


class DatabaseTest extends FunSuite with Matchers {

  val connectionPool = Mockito.mock(classOf[ConnectionPool], RETURNS_MOCKS)

  val database = new Database(connectionPool)

  import database._


  test("Database should read one record") {

    val connection = getConnection()
    val statement = Mockito.mock(classOf[PreparedStatement], RETURNS_MOCKS)
    when(connection.prepareStatement("select ? as key, ? as value from dual")).thenReturn(statement)
    setParameterCount(statement, 2)

    val resultSet = Mockito.mock(classOf[ResultSet], RETURNS_MOCKS)
    when(statement.executeQuery()).thenReturn(resultSet)

    when(resultSet.next()).thenReturn(true, true, true, true, true, true, false)
    when(resultSet.getString("key")).thenReturn("1", "2", "1", "1", "3", "3")
    when(resultSet.getInt("value")).thenReturn(1, 2, 3, 4, 5, 6)

    val key = 1
    val value = 2


    val result = database execute query (
      sql"select $key as key, $value as value from dual",
      toSetMultiMap(_.getString("key"), _.getInt("value")))


    result should be (Map("1" -> Set(1,4,3), "2" -> Set(2), "3" -> Set(5,6)))

    verify(connection, times(1)).prepareStatement("select ? as key, ? as value from dual")
    verify(statement, times(1)).setObject(1, key)
    verify(statement, times(1)).setObject(2, value)
    verify(resultSet, times(7)).next()
    verify(resultSet, times(6)).getString("key")
    verify(resultSet, times(6)).getInt("value")
  }

  test("Database should combine updates") {

    val connection = getConnection()
    val statement = Mockito.mock(classOf[PreparedStatement], RETURNS_MOCKS)
    when(connection.prepareStatement("update dual set value = ? where key = ?")).thenReturn(statement)
    setParameterCount(statement, 2)
    when(statement.executeUpdate()).thenReturn(7)

    val key1 = 1
    val value1 = 2

    val key2 = 3
    val value2 = 4


    database execute
      update (sql"update dual set value = $value1 where key = $key1") ~
      update (sql"update dual set value = $value2 where key = $key2")


    verify(connection, times(2)).prepareStatement("update dual set value = ? where key = ?")
    verify(statement, times(1)).setObject(1, value1)
    verify(statement, times(1)).setObject(2, key1)
    verify(statement, times(1)).setObject(1, value2)
    verify(statement, times(1)).setObject(2, key2)
    verify(statement, times(2)).executeUpdate()
  }

  test("Database should call procedures") {

    val connection = getConnection()
    val statement = Mockito.mock(classOf[CallableStatement], RETURNS_MOCKS)
    when(connection.prepareCall(
         """begin
              pack.proc(
                param1 => ?,
                param2 => ?,
                param3 => ?
              );
            end;""")).thenReturn(statement)
    setParameterCount(statement, 3)
    when(statement.execute()).thenReturn(true)
    when(statement.getObject(2)).thenReturn(java.lang.Integer.valueOf(34), Seq())


    val key = "key"

    val Seq(param2: Int) = database execute call(
      sql"""begin
              pack.proc(
                param1 => $key,
                param2 => ${out(Types.INTEGER)},
                param3 => ${123}
              );
            end;""")


    param2 should be (34)
    verify(connection, times(1)).prepareCall(
         """begin
              pack.proc(
                param1 => ?,
                param2 => ?,
                param3 => ?
              );
            end;""")
    verify(statement, times(1)).setString(1, key)
    verify(statement, times(1)).registerOutParameter(2, Types.INTEGER)
    verify(statement, times(1)).setInt(3, 123)
    verify(statement, times(1)).execute()
  }

  test("Database should support batch updates") {

    val connection = getConnection()
    val statement = Mockito.mock(classOf[PreparedStatement], RETURNS_MOCKS)
    when(connection.prepareStatement("update users set role = ? where id = ?")).thenReturn(statement)
    setParameterCount(statement, 2)
    when(statement.execute()).thenReturn(true)

    val userRoles = Seq(
      "qwerty" -> 1,
      "asdfgh" -> 2,
      "zxcvbn" -> 3
    )


    database execute batchUpdate( userRoles map {
      case (id: String, role: Int) => sql"update users set role = $role where id = $id"
    })


    verify(connection, times(1)).prepareStatement("update users set role = ? where id = ?")
    verify(statement, times(1)).setObject(1, 1)
    verify(statement, times(1)).setObject(2, "qwerty")
    verify(statement, times(1)).setObject(1, 2)
    verify(statement, times(1)).setObject(2, "asdfgh")
    verify(statement, times(1)).setObject(1, 3)
    verify(statement, times(1)).setObject(2, "zxcvbn")
    verify(statement, times(3)).addBatch()
    verify(statement, times(1)).executeBatch()
  }

  private def getConnection() = {
    make(Mockito.mock(classOf[Connection], RETURNS_MOCKS)) { connection: Connection =>
      when(connectionPool.getConnection).thenReturn(connection)
    }
  }

  private def setParameterCount(statement: PreparedStatement, parameterCount: Int) = {
    val metaData = Mockito.mock(classOf[ParameterMetaData], RETURNS_MOCKS)
    when(statement.getParameterMetaData()).thenReturn(metaData)
    when(metaData.getParameterCount()).thenReturn(parameterCount)
  }
}
