package com.wang930126.app.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.util.{MyKafkaSink, OffsetManager, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
将Maxwell从业务数据库中监测到的数据发送到Kafka的Ods层，进行分流
* */
object DB2Kafka_OdsApp_Maxwell {

    val groupid = "gmall_base_db_group_maxwell"
    val topic = "GMALL0105_DB_Maxwell"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        conf.set("spark.streaming.stopGracefullyOnShutdown","true")
        conf.set("spark.streaming.kafka.maxRatePerPartition","500")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        // TODO 1.去redis中取出偏移量放到一个Map中 根据这个Map创建kafka流
        val offsetMap = OffsetManager.getOffset(groupid,topic)
        val kafkaParams = Map[String,Object](
            "bootstrap.servers" -> PropertiesUtil.getConfig("kafka.server"),
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> groupid,
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> "false"
        )

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

        // TODO 2.取出偏移量 并 过滤掉两个不对的type
        // {"database":"gmall-2020-04",
        // "table":"z_user_info",
        // "type":"insert",
        // "ts":1589385314,
        // "xid":82982,
        // "commit":true,
        // "data":{"id":31,"user_name":"li4","tel":"1389999999"}}

        var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()
        val getOffsetRangeStream: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        val jsonObjStream: DStream[JSONObject] = getOffsetRangeStream.map(
            record => {
                val jsonStr: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                jsonObj
            }
        )

        val filteredJsonObjStream: DStream[JSONObject] = jsonObjStream.filter(
            jsonObj => {
                val `type`: String = jsonObj.getString("type")
                println(jsonObj)
                if (`type` == "bootstrap-start" || `type` == "bootstrap-complete") false else true
            }
        )

        // TODO 3. 再写回Kafka 分流 并提交偏移量
        filteredJsonObjStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    jsonObjPartition => {
                        val conn = RedisUtil.getResource()
                        try{
                            for (jsonObj <- jsonObjPartition) {
                                val tableName = jsonObj.getString("table").toUpperCase()
                                val topic = "ODS_T_" + tableName
                                val data = jsonObj.getJSONObject("data")
                                MyKafkaSink.sink(topic,data.getString("id"),data.toJSONString)
                            }
                        }catch{
                            case e:Exception =>
                        }finally{
                            RedisUtil.returnResource(conn)
                        }
                    }
                )
                OffsetManager.saveOffsetRanges(topic,groupid,offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()



    }
}
