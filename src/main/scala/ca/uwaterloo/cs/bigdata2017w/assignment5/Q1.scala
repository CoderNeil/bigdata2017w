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


class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date, text, parquet)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text format: " + args.text())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder().getOrCreate()

    val date = args.date()
    val count = sc.accumulator(0)

    if (args.text()){
      var fileName = ""
      fileName = "/lineitem.tbl"

      val textFile = sc.textFile(args.input() + fileName)

      textFile
        .foreach(line => {
          if (line.split("\\|")(10).contains(date)) {
            count += 1
          }
        })
    }
    else {

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      lineitemRDD
        .foreach(line => {
          if (line(10) == date) {
            count += 1
          }
        })
    }
    println("ANSWER=" + count.value)
  }
}

