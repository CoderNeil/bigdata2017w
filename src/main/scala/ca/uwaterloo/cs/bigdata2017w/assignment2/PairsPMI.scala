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

package ca.uwaterloo.cs.bigdata2017w.assignment2

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
import scala.collection.mutable.ListBuffer
import scala.math.log10


class Conf2(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

class MyPartitioner1(partitions: Int) extends Partitioner {
  require(partitions >= 0)
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = key match {
    case null => 0
    case (key1,key2) => (key1.hashCode & Integer.MAX_VALUE) % numPartitions
  }
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    var marginal = 0.0
    // var sum = 0.0
    var linesCount = textFile.count()
    val words = textFile
      .flatMap(line => {
        var wordAppear:Map[String,String] = Map()
        val tokens = tokenize(line).take(40)
        if (tokens.length > 1) {
        	val single = tokens.map{
            p => {
              if (wordAppear.contains(p)){
              }
              else {
                wordAppear = wordAppear + (p -> "*")
                p
              }
            }
          }
          single
        }
        else{
          List()
        }
    })
      .map(bigram => {
        // println (bigram)
        (bigram, 1)
      })
      .countByKey

      val pair = textFile
      .flatMap(line => {
        var wordAppear:Map[String,String] = Map()
        val tokens = tokenize(line).take(40)
        if (tokens.length > 1) {
          val single = tokens.map{
            p => {
              if (wordAppear.contains(p)){
              }
              else {
                wordAppear = wordAppear + (p -> "*")
              }
            }
          }

          val aList = new ListBuffer[(String,String)]()
          for (i <- wordAppear){
            for (j <- wordAppear){
              if (i != j){
                aList += ((i._1, j._1))
              }
            }
          }
        	aList.toList
        }
        else {
         	List()
     	  }
      })
      // println ("========================================")
      // println (counts)
      .map(bigram => {
        // println (bigram)
      	(bigram, 1.0)
      })
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitioner1(args.reducers()))
      // println ("========================================")
      .map (bigram => {
        // println (bigram._1._1)
        val x = words.get(bigram._1._1).get
        val y = words.get(bigram._1._2).get
        val sum = bigram._2
        val pmi = log10((sum * linesCount)/(x * y))
        // println(x, y, sum, pmi, linesCount)
        (bigram._1,(pmi, sum))
        })
      .map( bigram => {
        ("(" + bigram._1._1 + ", " + bigram._1._2 + ")") + " " + ("(" + bigram._2._1 + ", " + bigram._2._2 + ")")
        })
    .saveAsTextFile(args.output())
  }
}

