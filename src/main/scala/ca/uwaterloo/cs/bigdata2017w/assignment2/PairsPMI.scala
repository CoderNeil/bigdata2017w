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
    var totalLines = 0
    val counts = textFile
      .flatMap(line => {
        var wordAppear:Map[String,String] = Map()
        val tokens = tokenize(line).take(40)
        if (tokens.length > 1) {
        	val single = tokens.init.map{
            p => {
              if (wordAppear.contains(p)){
              }
              else {
                wordAppear = wordAppear + (p -> "*")
                (p, "*")
              }
            }
          }
          val lineNum = Map("***" -> "*")
        	single ++ lineNum
        }
        else {
         	List()
     	  }
      })
      .map(bigram => {
      	(bigram, 1)
      })
      .reduceByKey(_ + _)
      // .repartitionAndSortWithinPartitions(new MyPartitioner1(args.reducers()))
      // .map( pair => pair._1 match {
      //   case (_,"*") => {
      //     (pair._1, pair._2)
      //   }
      //   case ("***","*") => {
      //     totalLines = totalLines + 1
      //   }
      // })
      .flatMap( pair => {
        if (pair._1 == "***"){
          totalLines = totalLines + 1
        }else{
          (pair._1, pair._2)
        }
      })
      println (totalLines)
      val secondMapper = counts.foreach (pair => {
          counts.foreach (p => {
            if (pair._1 == p._1){}
            else {
              (pair._1,p._1)
            }
          })
      })
      .map(bigram => {
        (bigram, 1)
      })
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new MyPartitioner1(args.reducers()))
      .map( bigram => bigram._1 match {
      	case (_,"*") => {
      		marginal = bigram._2
          (bigram._1, bigram._2)
      	}
      	case (_,_) => {
          (bigram._1, bigram._2 / marginal)
      	} 
      	
      })
      .map( bigram => {
        ("(" + bigram._1._1 + ", " + bigram._1._2 + ")") + " " + bigram._2
        })
    counts.saveAsTextFile(args.output())
  }
}

