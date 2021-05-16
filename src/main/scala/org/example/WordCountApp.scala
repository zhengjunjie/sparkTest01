/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WordCountApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    //args(0)表示传进来的第一个参数
    val textFile = sc.textFile("C:\\Users\\zheng\\Desktop\\OS\\spark-2.3.0\\conf\\log4j.properties")
    val wc = textFile.flatMap(line => line.split(" ")).map((_,1)).reduceByKey(_+_).sortByKey(true)
    //打印出来
    wc.collect().foreach(println)
    sc.stop()
  }
}