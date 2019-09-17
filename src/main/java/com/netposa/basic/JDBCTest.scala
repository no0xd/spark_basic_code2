package com.netposa.basic

object JDBCTest {

  def main(args: Array[String]): Unit = {
    val resultSet = JDBCUtil.executeQuery("select * from ta")

    println(resultSet)
  }

  val conn = JDBCUtil.getConn();
  if (conn != null) {
    println(conn)
  }
}
