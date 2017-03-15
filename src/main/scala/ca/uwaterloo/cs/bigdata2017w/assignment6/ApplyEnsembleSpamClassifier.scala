package ca.uwaterloo.cs.bigdata2017w.assignment6

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
import scala.math.exp


class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model, method, output)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val method = opt[String](descr = "method", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // w is the weight vector (make sure the variable is within scope)
  val w = scala.collection.mutable.Map[Int,Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int], abroadcast: scala.collection.Map[Int, Double]) : Double = {
    var score = 0d
    features.foreach(f => if (abroadcast.contains(f)) score += abroadcast(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())
    log.info("Output: " + args.output())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // This is the main learner:
    // val delta = 0.002
    val method = args.method()
    val xFile = sc.textFile(args.model() + "/part-00000")
    .map( line => {
      val tokens = line.split(",")
      (tokens(0).substring(1, tokens(0).length).toInt, tokens(1).substring(0, tokens(1).length - 1).toDouble)
      })
    .collectAsMap()

    val yFile = sc.textFile(args.model() + "/part-00001")
    .map( line => {
      val tokens = line.split(",")
      (tokens(0).substring(1, tokens(0).length).toInt, tokens(1).substring(0, tokens(1).length - 1).toDouble)
      })
    .collectAsMap()

    val bFile = sc.textFile(args.model() + "/part-00002")
    .map( line => {
      val tokens = line.split(",")
      (tokens(0).substring(1, tokens(0).length).toInt, tokens(1).substring(0, tokens(1).length - 1).toDouble)
      })
    .collectAsMap()

    val xbroadcast = sc.broadcast(xFile)
    val ybroadcast = sc.broadcast(yFile)
    val bbroadcast = sc.broadcast(bFile)

    val train = sc.textFile(args.input())
    .map(line => {
      // Parse input
      // ..
      val tokens = line.split(" ")
      // For each instance...
      val docid = tokens(0)
      val features = tokens.drop(2).map(_.toInt) // feature vector of the training instance
      val scorex = spamminess(features, xbroadcast.value)
      val scorey = spamminess(features, ybroadcast.value)
      val scoreb = spamminess(features, bbroadcast.value)
      var isSpam = "ham"   // label
      var totalScore = 0.0
      if (method == "average"){
        totalScore = (scorex + scorey + scoreb) / 3
      }
      else {
        var votes = 0
        if (scorex > 0){
          votes += 1
        }
        else {
          votes -= 1
        }
        if (scorey > 0){
          votes += 1
        }
        else {
          votes -= 1
        }
        if (scoreb > 0){
          votes += 1
        }
        else {
          votes -= 1
        }
        totalScore = votes
      }
      if (totalScore > 0){
        isSpam = "ham"
      }
      (docid, tokens(1), totalScore, isSpam)
    })

      // Then run the trainer...

    train.saveAsTextFile(args.output())
  }
}