package org.pjgg.sparkStreamming

import java.util.Properties

import com.twitter.bijection.avro.SpecificAvroCodecs.{toJson, toBinary}
import com.typesafe.config.ConfigFactory
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.pjgg.sparkStreamming.TwitterStream.OnTweetPosted
import org.pjgg.sparkStreamming.avro.Tweet

import twitter4j.{Status, FilterQuery}


object KafkaProducerApp {

  private val conf = ConfigFactory.load()

  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String, Array[Byte]](config)
  }

  //LON - LAT = Spain with spanish language
  val filterEsOnly = new FilterQuery().locations(
    Array(-9.75585937,35.67514744),
    Array(4.30664063,42.42345652))
    //.language("en")

  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(toTweet(s))))
    //Start listening to tweets using our filter!
    twitterStream.filter(filterEsOnly)
  }

  private def toTweet(s: Status): Tweet = {
    new Tweet(s.getUser.getName, s.getText)
  }

  private def sendToKafka(t:Tweet) {
    println(toJson(t.getSchema).apply(t))
    val tweetEnc = toBinary[Tweet].apply(t)
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, tweetEnc)
    kafkaProducer.send(msg)
  }

}
