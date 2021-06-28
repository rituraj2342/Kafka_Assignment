package com.knoldus

import java.util.{ Collections, Properties}
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.io._

import scala.collection.JavaConverters._

object Consumer {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("test")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", classOf[CustomDeserializer])
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    println("inside consumer")
    val consumer: KafkaConsumer[String, User] = new KafkaConsumer[String, User](props)
    consumer.subscribe(Collections.singletonList(topic))
    while (true) {
      val record = consumer.poll(1000).asScala.iterator
      def writeFile(filename:String, record:Seq[String]): Unit = {
        val file = new File("/home/knoldus/Kafka_Assignment/src/main/scala/com/knoldus/text_file.txt")
        val bw = new BufferedWriter(new FileWriter(file))
        for (value <- record) {
          bw.write(value)
          println(value)
        }
        bw.close()
      }
    }

  }
}

