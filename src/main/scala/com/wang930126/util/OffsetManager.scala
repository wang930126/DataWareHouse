package com.wang930126.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

object OffsetManager {

    // offsetMap 的格式  key:offset_groupid value:Map(topic:partition -> offset)
    def getOffset(groupid: String, topic: String) = {
        val conn = RedisUtil.getResource()
        val offsetMap = mutable.Map[TopicPartition,Long]()
        try{
            val key = "offset_" + groupid
            import scala.collection.JavaConversions._
            val map: Map[String, String] = conn.hgetAll(key).toMap
            for (elem <- map) {
                val topic_partition = new TopicPartition(elem._1.split(":")(0),elem._1.split(":")(1).toInt)
                val offset = elem._2.toLong
                offsetMap.put(topic_partition,offset)
            }
        }catch{
            case e:Exception =>
        }finally{
            RedisUtil.returnResource(conn)
        }
        offsetMap
    }


    // offsetMap 的格式  key:offset_groupid value:Map(topic:partition -> offset)
    def saveOffsetRanges(topic: String, groupid: String, ranges: Array[OffsetRange]) = {
        val conn = RedisUtil.getResource()
        val key = "offset_" + groupid
        try{
            for (range <- ranges) {
                val field = topic + ":" + range.partition.toString
                conn.hset(
                    key,
                    field,
                    range.untilOffset.toString
                )
            }
        }catch{
            case e:Exception =>
        }finally{
            RedisUtil.returnResource(conn)
        }
    }

}
