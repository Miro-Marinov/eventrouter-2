package kafka

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.kafka._
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{Flow, Sink}
import json.EventJsonProtocol
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}
import spray.json.{JsString, JsValue}

case class KafkaConfig(system: ActorSystem) extends SprayJsonSupport with EventJsonProtocol {

  val bootstrapServers: String = system.settings.config.getString("akka.kafka.bootstrap-servers")
  // See application.conf: akka.kafka.consumer
  val consumerSettings: ConsumerSettings[Array[Byte], String] = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(bootstrapServers)
    .withGroupId("grp-1")
    .withClientId("client-1")

  val producerSettings: ProducerSettings[Array[Byte], String] = ProducerSettings(system, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(bootstrapServers)

  def toKafkaSink: Sink[JsValue, NotUsed] =
    Flow[JsValue]
      .map { msg =>
        val topic = getTopic(msg).getOrElse("deadLetter")
        println(s"Sending ${msg.toString()} to kafka topic $topic")
        new ProducerRecord[Array[Byte], String](topic, msg.toString())
      }
      .to(Producer.plainSink(producerSettings))

  // get the header.topic field
  def getTopic(msg: JsValue) = {
    val topic = for {
      header <- msg.asJsObject.fields.get("header")
      topic <- header.asJsObject().fields.get("topic")
    } yield topic

    topic.collect { case JsString(v) => v }
  }

}
