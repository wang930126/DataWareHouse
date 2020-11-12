package com.wang930126.app.userBehavior

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.DauInfo
import com.wang930126.util.{JsonUtil, MyKafkaUtil, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DauApp {

    val groupid = "dauapp_group"
    val topics = "dauapp_topic"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf()
                .setMaster("local[*]")
                .setAppName(this.getClass.getSimpleName)

        conf.set("spark.streaming.stopGracefullyOnShutdown","true")
        conf.set("spark.streaming.kafka.maxRatePerPartition","50")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        // 1.TODO 从redis中获取到偏移量 根据偏移量创建Kafka流
        // redis 保存偏移量如下 hash key:offset_groupid value = {topic:partition -> offset} string hash set zset list
        val offsetMap = mutable.Map[TopicPartition,Long]()
        val conn: Jedis = RedisUtil.getResource()
        try{
            val hKey = "offset_" + groupid
            import scala.collection.JavaConversions._
            val cachedOffset = conn.hgetAll(hKey).toMap
            cachedOffset.foreach{
                case (topic_partition,offset) => {
                    val tp: TopicPartition = new TopicPartition(topic_partition.split(":")(0),topic_partition.split(":")(1).toInt)
                    offsetMap.put(tp,offset.toLong)
                }
            }
        }catch{
            case e:Exception =>
        }finally{
            RedisUtil.returnResource(conn)
        }

        val kafkaParams = Map[String,Object](
            "bootstrap.servers" -> PropertiesUtil.getConfig("kafka.server"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> "true"
        )

        val kafkaStream = if(offsetMap.isEmpty)
                            MyKafkaUtil.getKafkaConnection(ssc,kafkaParams,topics)
                          else
                            MyKafkaUtil.getKafkaConnection(ssc,kafkaParams,offsetMap.toMap,topics)

        var offsetRanges = Array[OffsetRange]()

        val offsetGottenDStream: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )



        // kafka中json数据 格式如下
        //{"mid":"mid_55","uid":"uid_155","ar":"华北","ch":"Apple","vc":"0.1.5","ts":"11"}

        // TODO 过滤 清洗 转换成样例类
        val dauInfoStream: DStream[DauInfo] = offsetGottenDStream.filter(record => JsonUtil.isValidateJson(record.value()))
                .map(
                    record => {
                        val jsonObj: JSONObject = JSON.parseObject(record.value())
                        DauInfo(
                            jsonObj.getString("mid"),
                            jsonObj.getOrDefault("uid", "").toString,
                            jsonObj.getOrDefault("ar", "").toString,
                            jsonObj.getOrDefault("ch", "").toString,
                            jsonObj.getOrDefault("vc", "").toString,
                            null,
                            null,
                            null,
                            jsonObj.getLong("ts")
                        )
                    }
                )



        // TODO 流中的每条数据去redis中进行过滤 如果不在 就放进redis中 并通过 如果在redis中 就过滤
        // redis的 数据类型选择 set key为dau_20201107 value存放所有的mid_55

        dauInfoStream.foreachRDD(
            rdd => {

                // 处理业务
                rdd.mapPartitions(
                    dauInfoIter => {
                        val conn: Jedis = RedisUtil.getResource()
                        val dauList:ListBuffer[DauInfo] = ListBuffer[DauInfo]()
                        val key = "dau_" + new SimpleDateFormat("yyyyMMdd").format(new Date())
                        try{
                            dauInfoIter.foreach(
                                dauInfo => {
                                    println(key)
                                    println(dauInfo)
                                    val res: lang.Long = conn.sadd(key,dauInfo.mid)
                                    println(res)
                                    if(res == 1L){
                                        dauList.append(dauInfo)
                                    }
                                }
                            )
                        }catch{
                            case e:Exception =>
                        }finally{
                            RedisUtil.returnResource(conn)
                        }
                        dauList.toIterator
                    }
                ).foreachPartition(
                    partition => println(partition)
                )



                // 偏移量保存回redis drive执行
                val conn = RedisUtil.getResource()
                try{
                    val key = "offset_" + groupid
                    for (off <- offsetRanges) {
                        //key:offset_groupid value = {topic:partition -> offset}
                        val field = off.topic + ":" + off.partition.toString
                        val value = off.untilOffset.toString
                        conn.hset(key,field,value)
                    }
                }catch{
                    case e:Exception =>
                }finally{
                    RedisUtil.returnResource(conn)
                }

            }
        )

        ssc.start()
        ssc.awaitTermination()



    }

}
