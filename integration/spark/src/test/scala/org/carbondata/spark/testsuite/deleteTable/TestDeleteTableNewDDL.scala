package org.carbondata.spark.testsuite.deleteTable

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
  * test class for testing the delete table DDL.
  */
class TestDeleteTableNewDDL extends QueryTest with BeforeAndAfterAll {

  override def beforeAll: Unit = {

    sql("CREATE TABLE IF NOT EXISTS table1(empno Int, empname Array<String>, designation String, doj Timestamp, "
      + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
      + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
      + " STORED BY 'org.apache.carbondata.format' ")
    sql("CREATE TABLE IF NOT EXISTS table2(empno Int, empname Array<String>, designation String, doj Timestamp, "
      + "workgroupcategory Int, workgroupcategoryname String, deptno Int, deptname String, projectcode Int, "
      + "projectjoindate Timestamp, projectenddate Timestamp , attendance Int,utilization Int,salary Int )"
      + " STORED BY 'org.apache.carbondata.format' ")

  }

  // normal deletion case
  test("drop table Test with new DDL") {
    sql("drop table table1")

  }

  // deletion case with if exists
  test("drop table if exists Test with new DDL") {
    sql("drop table if exists table2")

  }

  // try to delete after deletion with if exists
  test("drop table after deletion with if exists with new DDL") {
    sql("drop table if exists table2")

  }

  // try to delete after deletion with out if exists. this should fail
  test("drop table after deletion with new DDL") {
    try {
      sql("drop table table2")
      fail("failed") // this should not be executed as exception is expected
    }
    catch {
      case e: Exception => // pass the test case as this is expected
    }
  }

  test("test delete segments by load date with case-insensitive tablename") {
    val pwd = new File(this.getClass.getResource("/").getPath + "/../../").getCanonicalPath
    val filePath = pwd + "/src/test/resources/emptyDimensionData.csv"
    sql(
      """
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'org.apache.carbondata.format'
           TBLPROPERTIES('DICTIONARY_EXCLUDE'='country,phonetype,serialname',
           'DICTIONARY_INCLUDE'='ID')
      """)

    sql(
      s"""
           LOAD DATA LOCAL INPATH '$filePath' into table t3
           """)

    checkAnswer(
      sql("select count(*) from t3"), Seq(Row(20)))

    sql("delete segments from table t3 where starttime before '2099-07-28 11:00:00'")

    checkAnswer(
      sql("select count(*) from t3"), Seq(Row(0)))

    //drop test table
    sql("drop table t3")
  }
  override def afterAll: Unit = {

  }

}
