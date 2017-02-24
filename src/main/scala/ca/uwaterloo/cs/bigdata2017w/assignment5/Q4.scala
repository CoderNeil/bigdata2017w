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
  // val text = opt[Boolean](descr = "text format", required = false, default = Some(true))
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

    var fileName = ""
    val date = args.date()
      fileName = "/lineitem.tbl"

    val lineItemFile = sc.textFile(args.input() + fileName)
      .filter(line => {
          line.split("\\|")(10).contains(date)
        })
      .map( line => {
        (line.split("\\|")(0), 0)
      })

        fileName = "/orders.tbl"

    val orderFile = sc.textFile(args.input() + fileName)
      .map( line => {
        val temp = line.split("\\|")
        (temp(0), temp(1))
        })

      fileName = "/customer.tbl"

    val customerFile = sc.textFile(args.input() + fileName)
      .map(line => {
        val temp = line.split("\\|")
        (temp(0), temp(3))
        })
      .collectAsMap()

    fileName = "/nation.tbl"
    val nationFile = sc.textFile(args.input() + fileName)
      .map(line => {
        val temp = line.split("\\|")
        (temp(0), temp(1))
        })
      .collectAsMap()

    val cBroadcast = sc.broadcast(customerFile)
    val nBroadcast = sc.broadcast(nationFile)

    val output = orderFile.cogroup(lineItemFile)
      .filter(line => {
          !line._2._2.isEmpty
        })
      .map( line => {
        val temp = cBroadcast.value(line._2._1.iterator.next())
        (temp, 1)
        })
      .reduceByKey(_+_)
      .sortByKey(true)
    output
      .foreach(line => {
        println("(" + line._1  + ", " + nBroadcast.value(line._1) + ", " + line._2 + ")")
        })
  }
}

