package com.wang930126.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MyKafkaUtil {

    def getKafkaConnection(ssc:StreamingContext,kafkaParams:Map[String,Object],topics:String*) = {
        KafkaUtils.createDirectStream[String,String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](
                topics,
                kafkaParams
            )
        )
    }

    def getKafkaConnection(ssc:StreamingContext, kafkaParams:Map[String,Object], offsetMap:Map[TopicPartition,Long], topics:String*) = {
        KafkaUtils.createDirectStream[String,String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](
                topics,
                kafkaParams,
                offsetMap
            )
        )
    }

}
