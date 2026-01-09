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

/** paimon spark test. */
class AskwangPaimonDataFrameTest extends PaimonSparkTestBase {

  /**
   *   - sql 语法解析，切换到 paimon write
   *   - write 框架实现
   *   - data 写入，manifest 写入，commit 逻辑
   *   - 不同 file.format 和 file.compression.
   */
  test("insert into non-partition/no-bucket/append table") {

    println(sparkVersion)

    sql(s"""
           |CREATE TABLE T (id STRING, name STRING)
           |TBLPROPERTIES ('file.format'='parquet')
           |""".stripMargin)

    val df = spark.emptyDataFrame
    df.write.insertInto("T")
  }
}
