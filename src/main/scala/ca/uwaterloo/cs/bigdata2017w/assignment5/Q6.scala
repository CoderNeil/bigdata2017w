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


class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  // val text = opt[Boolean](descr = "text format", required = false, default = Some(true))
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    // log.info("Text format: " + args.text())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

    var fileName = ""
    val date = args.date()
      fileName = "/lineitem.tbl"

    val lineItemFile = sc.textFile(args.input() + fileName)
      .map(line => {
        line.split("\\|")
        })
      .filter(line => {
          line(10).contains(date)
        })
      .keyBy(line => {
        (line(8), line(9))
        })
      .groupByKey()

    val sumQty = lineItemFile
      .map(line => {
        line._2.iterator.map(lines => {
          lines(4).toDouble
          })
        .toList.sum
        })
      .sum()

    val sumBasePrice = lineItemFile
      .map(line => {
        line._2.iterator.map(lines => {
          lines(5).toDouble
          })
        .toList.sum
        })
      .sum()

    val sumDiscount = lineItemFile
      .map(line => {
        line._2.iterator.map(lines => {
          lines(6).toDouble
          })
        .toList.sum
        })
      .sum()

    val sumDiscPrice = lineItemFile
      .map(line => {
        line._2.iterator.map(lines => {
          lines(5).toDouble * ( 1 - lines(6).toDouble)
          })
        .toList.sum
        })
      .sum()

    val sumCharge = lineItemFile
      .map(line => {
        line._2.iterator.map(lines => {
          lines(5).toDouble * ( 1 - lines(6).toDouble) * ( 1 + lines(7).toDouble)
          })
        .toList.sum
        })
      .sum()

    val count = lineItemFile.count()
    val avgQty = sumQty / count
    val avgPrice = sumBasePrice / count
    val avgDisc = sumDiscPrice / count
    
    val output = lineItemFile
      .collect()
      .foreach(line => {
        println("(" + line._1._1  + ", " + line._1._2 + ", " +
         sumQty + ", " + sumBasePrice + ", " + sumDiscPrice + ", " + 
         sumCharge + ", " + avgQty + ", " +  avgPrice + ", " + 
         avgDisc + ", " + count.toDouble + ")")
        })
  }
}

