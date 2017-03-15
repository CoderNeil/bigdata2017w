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


class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "model", required = true)
  val shuffle = opt[Boolean]()
  verify()
}
object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  // w is the weight vector (make sure the variable is within scope)
  val w = scala.collection.mutable.Map[Int,Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]) : Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // This is the main learner:
    val delta = 0.002

    var textFile = sc.textFile(args.input())

    val shuff = args.shuffle()

    if (shuff){
      textFile = textFile
      .map( line => {
        val num = scala.util.Random.nextInt()
        (num , line)
        })
      .sortByKey(true)
      .map( line => {
        (line._2)
        })
    }

    val trained = textFile.map(line => {
      // Parse input
      // ..
      val tokens = line.split(" ")
      // For each instance...
      val docid = tokens(0)
      var isSpam = 0   // label
      val features = tokens.drop(2).map(_.toInt) // feature vector of the training instance

      if (tokens(1) == "spam"){
        isSpam = 1
      }
      (0, (docid, isSpam, features))
    })
    .groupByKey(1)
    .flatMap( line => {
      line._2.foreach( lines => {
        // For each instance...
        val isSpam = lines._2   // label
        val features = lines._3 // feature vector of the training instance

        // Update the weights as follows:
        val score = spamminess(features)
        val prob = 1.0 / (1 + exp(-score))
        features.foreach(f => {
          if (w.contains(f)) {
            w(f) = (isSpam - prob) * delta + w(f)
          } else {
            w(f) = (isSpam - prob) * delta
           }
        })
      })
      w
    })

      // Then run the trainer...

    trained.saveAsTextFile(args.model())
  }
}