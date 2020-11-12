package com.wang930126.util

import java.io.InputStream
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object MyKafkaSink {

    var producer:KafkaProducer[String,String] = null

    def createKafkaProducer() = {
        val pro: Properties = new Properties()
        val path = "config.properties"
        val is: InputStream = MyKafkaUtil.getClass.getClassLoader.getResourceAsStream(path)
        pro.load(is)
        val properties: Properties = new Properties
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,pro.getProperty("kafka.server"))
        properties.put(ProducerConfig.ACKS_CONFIG,"-1")
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
        var producer:KafkaProducer[String,String] = null
        try{
            producer = new KafkaProducer[String,String](properties)
        }catch {
            case e :Exception =>
        }
        producer
    }

    def sink(topic:String,key:String,message:String):Unit = {
        if(producer == null){
            producer = createKafkaProducer()
        }
        producer.send(
            new ProducerRecord[String,String](
                topic,
                key,
                message
            )
        )
    }

}
