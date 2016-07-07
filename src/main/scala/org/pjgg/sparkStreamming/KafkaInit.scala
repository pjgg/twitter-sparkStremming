package org.pjgg.sparkStreamming

import com.typesafe.config.ConfigFactory
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object KafkaInit extends LazyLogging{

  private val conf = ConfigFactory.load()

  def main(args: Array[String]): Unit = {



    val numThreads = 1
    val topics = "test"
    val groupId = "groupId_1"
    val sc = getSparkContext
    val zkQuorum = conf.getString("kafka.zookeeper.quorum")
    val readParallelism = 5 // number of kafka partitions
    val processingParallelism = 1

    val	ssc	=	new	StreamingContext(sc,	batchDuration	=	Seconds(10))
    ssc.checkpoint("/tmp/checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    /*val kafkaStreams = (1 to readParallelism).map { _ =>KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap).map(_._2)}
    val unifiedStream = ssc.union(kafkaStreams)
    unifiedStream.repartition(processingParallelism)*/

    val unifiedStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap).map(_._2)
    val amount = unifiedStream.filter(_.contains("remains")).countByWindow(Seconds(10) ,Seconds(10))

    // We use accumulators to track global "counters" across the tasks of our streaming app
    val numRemindsMessages = ssc.sparkContext.accumulator[Long](0L, "remains messages consumed")

    amount.foreachRDD(r => r.take(1).foreach{ a =>
      println(a)
      numRemindsMessages += a
    })

    ssc.start()
    ssc.awaitTermination()

    println(s"Total remains calculated ${numRemindsMessages.value}")
  }

  private def getSparkContext():SparkContext = {
    new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))
  }
}
