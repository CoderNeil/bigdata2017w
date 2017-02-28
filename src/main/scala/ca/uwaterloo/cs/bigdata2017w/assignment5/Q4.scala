/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs.bigdata2017w.assignment5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import collection.mutable.HashMap
import scala.collection.JavaConverters._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.sql.SparkSession


class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    // log.info("Text format: " + args.text())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()

    var fileName = ""
    val date = args.date()

    if (args.text()) {
      fileName = "/lineitem.tbl"

      val lineItemFile = sc.textFile(args.input() + fileName)
        .filter(line => {
            line.split("\\|")(10).contains(date)
          })
        .map( line => {
          (line.split("\\|")(0).toInt, 0)
        })

          fileName = "/orders.tbl"

      val orderFile = sc.textFile(args.input() + fileName)
        .map( line => {
          val temp = line.split("\\|")
          (temp(0).toInt, temp(1))
          })

        fileName = "/customer.tbl"

      val customerFile = sc.textFile(args.input() + fileName)
        .map(line => {
          val temp = line.split("\\|")
          (temp(0).toInt, temp(3))
          })
        .collectAsMap()

      fileName = "/nation.tbl"
      val nationFile = sc.textFile(args.input() + fileName)
        .map(line => {
          val temp = line.split("\\|")
          (temp(0).toInt, temp(1))
          })
        .collectAsMap()

      val cBroadcast = sc.broadcast(customerFile)
      val nBroadcast = sc.broadcast(nationFile)

      val output = orderFile.cogroup(lineItemFile)
        .filter(line => {
            !line._2._2.isEmpty
          })
        .map( line => {
          val temp = cBroadcast.value(line._2._1.iterator.next().toInt)
          (temp.toInt, 1)
          })
        .reduceByKey(_+_)
        .collectAsMap()
        .toList
        .sortBy(_._1)
      output
        .foreach(line => {
          println("(" + line._1  + "," + nBroadcast.value(line._1.toString.toInt) + "," + line._2 + ")")
          })
    }
    else {
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .filter(line => {
            line(10) == (date)
          })
        .map( line => {
          (line(0).toString.toInt, 0)
        })

          fileName = "/orders.tbl"

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
        .map( line => {
          (line(0).toString.toInt, line(1))
          })

        fileName = "/customer.tbl"

      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
        .map(line => {
          (line(0).toString.toInt, line(3))
          })
        .collectAsMap()

      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
        .map(line => {
          (line(0).toString.toInt, line(1))
          })
        .collectAsMap()

      val cBroadcast = sc.broadcast(customerRDD)
      val nBroadcast = sc.broadcast(nationRDD)

      val output = ordersRDD.cogroup(lineitemRDD)
        .filter(line => {
            !line._2._2.isEmpty
          })
        .map( line => {
          val temp = cBroadcast.value(line._2._1.iterator.next().toString.toInt)
          (temp.toString.toInt, 1)
          })
        .reduceByKey(_+_)
        .collectAsMap()
        .toList
        .sortBy(_._1)
      output
        .foreach(line => {
          println("(" + line._1  + "," + nBroadcast.value(line._1.toString.toInt) + "," + line._2 + ")")
          })
    }
  }
}

