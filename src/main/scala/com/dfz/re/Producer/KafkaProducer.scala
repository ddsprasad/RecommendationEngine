/*
  * The Object is used to read movies and ratings data from
  * hdfs as a stream of data to configured kafka topic.
  * Created by Spoorthy,Swapna & Bipasha on 9/25/2017.
  */
package com.dfz.re.Producer

import java.io.FileNotFoundException
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source

object KafkaProducer extends App{

  if(args.length != 4) {
    System.err.println("Usage: " + this.getClass.getSimpleName + " <Broker Number> <FilePath> <TopicName> <Flag>")
    System.exit(1)
  }
  val brokerNum = args(0).toString
  //path of the file
  val file = args(1).toString
  val topicName = args(2).toString
  val flag = args(3).toString

    /* Define properties for how the Producer finds the cluster,
  serializes the messages and if appropriate directs the message */
  val props = new Properties()
  props.put("bootstrap.servers", brokerNum)
  props.put("client.id", "ScalaProducerExample")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  //Define producer object,takes 2 params; first type of  key, second type of the message
  val producer = new KafkaProducer[String, String](props)

  try{
    //drops the header and reads line from csv file
    for (lines <- Source.fromFile(file).getLines().drop(1)) {

      val data = new ProducerRecord[String, String](topicName, lines + "," + flag)

      /*Finally write the message to broker (here in data,lines is the actual message,
      flag is used to differentiate the messages from two topics)*/
      producer.send(data)
      println("Sent Message: " + lines + "," + flag)
    }
  }
  catch {
    case e: FileNotFoundException=> println("File Not Found Exception");
  }
  producer.close()

}
