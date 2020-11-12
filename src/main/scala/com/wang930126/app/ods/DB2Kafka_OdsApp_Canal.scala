package com.wang930126.app.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.wang930126.util.{MyKafkaSink, OffsetManager, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable

/**
  * 完成ODS层原始数据的分流
  */
object DB2Kafka_OdsApp_Canal {

    val groupid = "gmall_base_db_group"
    val topic = "GMALL0105_DB_Canal"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        conf.set("spark.streaming.stopGracefullyOnShutdown","true")
        conf.set("spark.streaming.kafka.maxRatePerPartition","150")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        // TODO 1.从redis中获取offset 并创建kafka流
        val kafkaParams = Map[String,Object](
            "bootstrap.servers" -> PropertiesUtil.getConfig("kafka.server"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> "false"
        )

        val offsetMap = mutable.Map[TopicPartition,Long]()

        val conn: Jedis = RedisUtil.getResource()
        // offsetMap 的格式  key:offset_groupid value:Map(topic:partition -> offset)
        try {
            val key = "offset_" + groupid
            import scala.collection.JavaConversions._
            val map: Map[String, String] = conn.hgetAll(key).toMap
            map.foreach {
                case (topic_partition, offset) => {
                    val tp: TopicPartition = new TopicPartition(topic_partition.split(":")(0), topic_partition.split(":")(1).toInt)
                    offsetMap.put(tp, offset.toLong)
                }
            }
        }catch{
            case e:Exception =>
        }finally{
            RedisUtil.returnResource(conn)
        }

        val kafkaStream = if(offsetMap.isEmpty){
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(topic),
                    kafkaParams
                )
            )
        }else{
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(topic),
                    kafkaParams,
                    offsetMap
                )
            )
        }

        var ranges:Array[OffsetRange] = Array[OffsetRange]()
        // 获取本批次rdd的偏移量
        val gottenOffsetStream: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(
            rdd => {
                ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 从流中解析出canal发送来的json数据
        //{
        // "data":[{"id":"16","user_name":"zhang3","tel":"13810001010"},{"id":"17","user_name":"zhang3","tel":"13810001010"}],
        // "database":"gmall-2020-04",
        // "es":1589196502000,
        // "id":4,
        // "isDdl":false,
        // "mysqlType":{"id":"bigint(20)","user_name":"varchar(20)","tel":"varchar(20)"},
        // "old":null,
        // "pkNames":["id"],
        // "sql":"",
        // "sqlType":{"id":-5,"user_name":12,"tel":12},
        // "table":"z_user_info",
        // "ts":1589196502433,
        // "type":"INSERT"}
        val jsonObjStream: DStream[JSONObject] = gottenOffsetStream.map(
            record => {
                val jsonStr: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                println(jsonObj)
                jsonObj
            }
        )

        jsonObjStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    jsonObjPartition => {
                        // 数据写入kafka
                        for (jsonObj <- jsonObjPartition) {
                            val dataArray: JSONArray = jsonObj.getJSONArray("data")
                            val tableName = jsonObj.getString("table").toUpperCase()
                            val topic = "ODS_" + tableName
                            for(index <- 0 to dataArray.size() - 1){
                                val dataJson = dataArray.getJSONObject(index)
                                MyKafkaSink.sink(
                                    topic,
                                    tableName + dataJson.getString("id"),
                                    dataJson.toString
                                )

                            }
                        }
                    }
                )

                // 提交偏移量到redis
                OffsetManager.saveOffsetRanges(topic,groupid,ranges)
            }

        )

        ssc.start()
        ssc.awaitTermination()

    }
}
