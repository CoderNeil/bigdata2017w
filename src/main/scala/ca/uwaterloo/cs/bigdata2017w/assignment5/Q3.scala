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


class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    // log.info("Text format: " + args.text())

    val conf = new SparkConf().setAppName("Q3")
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

      fileName = "/part.tbl"

    val partFile = sc.textFile(args.input() + fileName)
      .map(line => {
        val temp = line.split("\\|")
        (temp(0), temp(1))
        })
      .collectAsMap()

    fileName = "/supplier.tbl"
    val supplierFile = sc.textFile(args.input() + fileName)
      .map(line => {
        val temp = line.split("\\|")
        (temp(0), temp(1))
        })
      .collectAsMap()

    val pBroadcast = sc.broadcast(partFile)
    val sBroadcast = sc.broadcast(supplierFile)

    val output = lineItemFile
      .map( line => {
        val temp = line.split("\\|")
        (temp(0), (pBroadcast.value.get(temp(1)), sBroadcast.value.get(temp(2))))
        })
      .sortByKey(true)
      .take(20)
    output
      .foreach(line => {
        println("(" + line._1  + ", " + line._2._1.get + ", " + line._2._2.get + ")")
        })
    }
    else {
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
        .filter(line => {
            line(10) == (date)
          })

        fileName = "/part.tbl"

      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
        .map(line => {
          (line(0).toString, line(1))
          })
        .collectAsMap()

      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd
        .map(line => {
          (line(0).toString, line(1))
          })
        .collectAsMap()

      val pBroadcast = sc.broadcast(partRDD)
      val sBroadcast = sc.broadcast(supplierRDD)

      val output = lineitemRDD
        .map( line => {
          (line(0).toString, (pBroadcast.value.get(line(1).toString), sBroadcast.value.get(line(2).toString)))
          })
        .collectAsMap()
        .toList
        .sortBy(_._1)
        .take(20)
      output
        .foreach(line => {
          println("(" + line._1  + ", " + line._2._1.get + ", " + line._2._2.get + ")")
          })
    }
  }
}

