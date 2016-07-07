package org.pjgg.sparkStreamming

import java.lang.CharSequence
import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs
import com.twitter.bijection.avro.SpecificAvroCodecs._
import com.typesafe.config.ConfigFactory
import kafka.producer.{ProducerConfig, KeyedMessage}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.pjgg.sparkStreamming.avro.{WordsCount, Tweet}
import kafka.javaapi.producer.Producer
import collection.JavaConversions._

object KafkaConsumerApp extends LazyLogging{

  private val conf = ConfigFactory.load()

  val topics = "tweets"
  val aggrTopic = "toMongo"
  val groupId = "groupId_1"

  def main(args: Array[String]): Unit = {

    val numThreads = 1
    val sc = getSparkContext
    val zkQuorum = conf.getString("kafka.zookeeper.quorum")
    val readParallelism = 5 // number of kafka partitions
    val processingParallelism = 1

    val	ssc	=	new	StreamingContext(sc,	batchDuration	=	Seconds(10))
    ssc.checkpoint("/tmp/checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val unifiedStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap).map(_._2)
    val tweets = unifiedStream.flatMap( x => SpecificAvroCodecs.toBinary[Tweet].invert(x.getBytes).toOption)

    val wordCounts = tweets.flatMap(_.getText.toString.split(" ")).map((_,1)).reduceByKey(_ + _)
    val wordsFiltered = wordCounts.filter(_._1.size > 4)
    val countsSorted = wordsFiltered.transform(_.sortBy(_._2, ascending = false))

    countsSorted.print(10)

    countsSorted.foreachRDD(r => sendToKafka(toWordsCount(mapAsJavaMap(r.collect().toMap[String,Int]))))

    //val amount = tweets.filter(_.getText.toString.contains("dia")).countByWindow(Seconds(10) ,Seconds(10))
   /* amount.foreachRDD(r => r.take(1).foreach{ a =>
      println(a)
    })*/

    ssc.start()
    ssc.awaitTermination()

  }

  private def sendToKafka(t:WordsCount) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[WordsCount].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](aggrTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  private def toWordsCount(aggr: java.util.Map[String,Int]): WordsCount = {
    val tmp:java.util.Map[java.lang.CharSequence,java.lang.Integer] = aggr.map{
      case (k,v) => (k.subSequence(0,k.length), new java.lang.Integer(v))
    }
    new WordsCount(tmp)
  }

  private def getSparkContext():SparkContext = {
    new SparkContext(new SparkConf().setAppName("SparkSQL").setMaster("local[*]"))
  }
}
