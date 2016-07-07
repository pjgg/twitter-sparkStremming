# Twitter-Spark Proof Of Concept

- KafkaProducerApp will consume Twitter Stream API, and drop tweets into Kafka (Avro format - topic tweets). It's filtered by Lon / Lat
- KafkaConsumerApp will process tweets from Kafka and count words greater than 3 characters. Result will be dropped into Kafka (Avro format - topic toMongo)

- KafkaInit it's a Mock of KafkaConsumerApp

In resources/profile/local/avro you will find avro schemas. To generate the code in order to convert message into Byte[] you will have to run first the following command mvn generate-sources 
code will be generated into org.pjgg.sparkStreamming.avro package It's configured in pom.xml

Kafka could be launch from resources/docker/start.sh but you will have to create the Topics 

