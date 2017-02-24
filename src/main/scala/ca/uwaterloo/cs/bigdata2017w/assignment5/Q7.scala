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


class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  // val text = opt[Boolean](descr = "text format", required = false, default = Some(true))
  verify()
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    // log.info("Text format: " + args.text())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

    var fileName = ""
    val date = args.date()
      fileName = "/lineitem.tbl"

    val lineItemFile = sc.textFile(args.input() + fileName)
      .filter(line => {
          line.split("\\|")(10) > (date)
        })
      .map( line => {
        val temp = line.split("\\|")
        (temp(0).toInt, (temp(5).toDouble * (1 - temp(6).toDouble)))
      })

    fileName = "/customer.tbl"
    val customerFile = sc.textFile(args.input() + fileName)
      .map(line => {
        val temp = line.split("\\|")
        (temp(0), temp(1))
        })
      .collectAsMap()

    val cBroadcast = sc.broadcast(customerFile)

    fileName = "/orders.tbl"
    val orderFile = sc.textFile(args.input() + fileName)
      .filter(line => {
        line.split("\\|")(4) < (date)
        })
      .map(line => {
        val temp = line.split("\\|")
        (temp(0).toInt, (cBroadcast.value(temp(1)), temp(4), temp(7)))
        })
      

    val output = orderFile.cogroup(lineItemFile)
      .filter(line => {
        !line._2._1.isEmpty && !line._2._2.isEmpty
        })
      .map( line => {
        val next = line._2._1.iterator.next()
        (line._2._2.foldRight(0.0)((a,b) => a + b),
          (next._1, line._1, next._2, next._3))
        })
      .sortByKey(false)
      .take(10)
    output
      .foreach(line => {
        val temp = 
        println("(" + line._2._1  + ", " + line._2._2 + ", " + 
          line._1 + ", " + line._2._3 + ", " + line._2._4 + ")")
        })
  }
}

