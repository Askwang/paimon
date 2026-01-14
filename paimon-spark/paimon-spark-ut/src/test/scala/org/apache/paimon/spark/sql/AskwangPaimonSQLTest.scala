/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.PaimonSparkTestBase

import org.apache.spark.sql.execution.QueryExecution

/** paimon spark test. */
class AskwangPaimonSQLTest extends PaimonSparkTestBase {

  // ----------------------------------- read and write ---------------------------------

  test("[read/write] insert into non-partition/no-bucket/append table") {

    println(sparkVersion)

    withSparkSQLConf("spark.sql.planChangeLog.level" -> "TRACE") {
      sql(s"""
             |CREATE TABLE T (id int, name string)
             |TBLPROPERTIES ('file.format'='parquet')
             |""".stripMargin)

      val df = sql("insert into T values(1,'wangkang')")

      AskwangUtils.printNumberTree(df.queryExecution)
      sql("select * from T").show(false)
    }
  }

  test("String hour with int type not partition push down") {

    println(sparkVersion)

    sql(s"""
           |CREATE TABLE T (id STRING, hour STRING, appid string)
           |TBLPROPERTIES ('primary-key'='id,hour', 'bucket'='2')
           | PARTITIONED BY (hour)
           |""".stripMargin)

    sql(" insert into T values ('1', '15', '004');")
    sql(" insert into T values ('1', '16', '004');")
    sql(" insert into T values ('1', '17', '003');")
    sql(" insert into T values ('2', '17', '004');")

    //    sql("select * from T where hour=17 and appid ='004'").show(false)
    sql("select * from T where hour='17' and appid ='004'").show(false)
  }

  test("cast(hour as int) push down") {

    println(sparkVersion)

    sql(s"""
           |CREATE TABLE T (id STRING, hour STRING, appid string, day string)
           |TBLPROPERTIES ('primary-key'='id,hour,day', 'bucket'='2')
           | PARTITIONED BY (hour,day)
           |""".stripMargin)

    sql(" insert into T values ('1', '15', '004', '2021');")
    sql(" insert into T values ('1', '16', '004', '2021');")
    sql(" insert into T values ('1', '17', '003', '2025');")
    sql(" insert into T values ('2', '17', '004', '2025');")

    //    sql("select * from T where hour=17 and appid ='004'").show(false)
//    sql("select * from T where hour='17' and appid ='004' and day = '2025' ").show(false)
    sql("select * from T where hour='17' and id = '2' and day = '2025' ").show(false)
  }

  // ----------------------------------- merge engine ---------------------------------

  test("[merge-engine] deduplicate with sequence.field") {
    sql("""
          | create table T (id string, merge_field bigint, phone string)
          | TBLPROPERTIES ('primary-key'='id', 'bucket'='1',
          | 'merge-engine'='deduplicate', 'sequence.field'='merge_field')
          |""".stripMargin)

    sql("insert into T values ('1', 1, 'a')")
    sql("insert into T values ('1', 1, 'c')")
    sql("insert into T values ('1', 0, 'b')")
  }

}

object AskwangUtils {
  def printNumberTree(qe: QueryExecution): Unit = {
    println("=== Parsed Logical Plan ===")
    println(qe.logical.numberedTreeString)

    println("=== Analyzed Logical Plan ===")
    println(qe.analyzed.numberedTreeString)

    println("=== Optimized Logical Plan ===")
    println(qe.optimizedPlan.numberedTreeString)

    println("=== Physical Plan ===")
    println(qe.sparkPlan.numberedTreeString)
  }
}
