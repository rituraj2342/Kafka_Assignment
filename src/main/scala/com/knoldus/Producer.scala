package com.knoldus

import java.util.Properties
import org.apache.kafka.clients.producer._

object Producer  {

  def main(args: Array[String]): Unit = {
    writeToKafka("test")
  }

  def writeToKafka(topic: String): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", classOf[CustomSerializer])
    val producer = new KafkaProducer[String, User](props)


    val name = "Rituraj"
    val age = "23"
    val course = "Btech"


    for (i <- 1 to 1) {
      val record = new ProducerRecord[String, User](topic, s"i", User(i, s"$name", s"$age", s"$course"))
      producer.send(record)
    }
    println("producer complete")
    producer.close()
  }
}




