package com.wang930126.app.dwd

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.{OrderInfo, UserInfo}
import com.wang930126.util._
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

import scala.collection.mutable


object OrderInfoApp {

    val groupid = "GMALL_ORDER_INFO_CONSUMER"
    val topic = "ODS_T_ORDER_INFO"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
        conf.set("spark.streaming.kafka.maxRatePerPartition","200")
        conf.set("spark.streaming.stopGracefullyOnShutdown","true")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

        // TODO 1.去redis中查看偏移量
        val offsetMap: mutable.Map[TopicPartition, Long] = OffsetManager.getOffset(groupid,topic)

        val kafkaParams = Map[String,Object](
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getConfig("kafka.server"),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.GROUP_ID_CONFIG -> groupid,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
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
        } else {
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

        // TODO 2.获取偏移量
        var offsetRanges = Array[OffsetRange]()
        val gottenOffsetRangesStream: DStream[ConsumerRecord[String, String]] = kafkaStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        // TODO 3.流转换成样例类流
        val orderInfoStream: DStream[OrderInfo] = gottenOffsetRangesStream.map(
            record => {
                val jsonStr: String = record.value()
                val obj: OrderInfo = JSON.parseObject(jsonStr,classOf[OrderInfo])
                val create_time: String = obj.create_time
                obj.create_date = create_time.split(" ")(0)
                obj.create_hour = (create_time.split(" ")(1)).split(":")(0)
                obj
            }
        )

        orderInfoStream.cache()

        // TODO 4.从hbase中查询状态 为每个orderinfo加上记号
        val orderInfoWithIfConsumedStream: DStream[OrderInfo] = orderInfoStream.mapPartitions(
            orderInfoPartitions => {
                val orderInfoList: List[OrderInfo] = orderInfoPartitions.toList
                if(orderInfoList.size != 0){
                    val conn = RedisUtil.getResource()
                    val uids: String = orderInfoList.map("'" + _.user_id + "'").distinct.mkString(",")
                    //println(uids)
                    val sql: String = "select * from USER_STATE where USER_ID in (" + uids + ")"
                    //println(sql)
                    val id2If_consumed: Map[String, String] = PhoenixUtil.query(sql, null)
                            .map(
                                jsonObj => (jsonObj.getString("USER_ID"), jsonObj.getString("IF_CONSUMED"))
                            ).toMap
                    try {
                        orderInfoList.foreach(
                            orderInfo => {
                                val state = id2If_consumed.getOrElse(orderInfo.user_id.toString, null)
                                // 如果状态值为0 或者 没有状态值 说明是首单
                                if (state == 0 || state == null) orderInfo.if_first_order = "1"
                                // 反之不是首单
                                else orderInfo.if_first_order = "0"
                            }
                        )
                    } catch {
                        case e: Exception => e.printStackTrace()
                    } finally {
                        RedisUtil.returnResource(conn)
                    }
                    orderInfoList.toIterator
                }else{
                    orderInfoPartitions
                }
            }
        )

        orderInfoWithIfConsumedStream.cache()

        // TODO 5.流中同一个用户的订单 如果第一单是首单了 那么后面的单就不该是首单了
        val groupByUidOrderInfoStream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithIfConsumedStream.map(orderinfo => (orderinfo.user_id, orderinfo))
                .groupByKey()

        groupByUidOrderInfoStream.cache()

        val modifiedWithIfConsumedOrderInfoStream: DStream[OrderInfo] = groupByUidOrderInfoStream.flatMap {
            case (userid, orderInfoIter) => {
                var orderInfoList: List[OrderInfo] = orderInfoIter.toList
                if (orderInfoList(0).if_first_order == "1") {
                    orderInfoList = orderInfoList.sortBy(_.create_time)
                    for(index <- 1 to orderInfoList.length - 1){
                        orderInfoList(index).if_first_order = "0"
                    }
                }
                orderInfoList
            }
        }

        // TODO 6.混入省市地区维度
        val id2Name_AreaCode: Map[String, (String, String)] = PhoenixUtil.query("select * from ODS_BASE_PROVINCE", null)
                .map(jsonObj => (jsonObj.getString("ID"), (jsonObj.getString("NAME"), jsonObj.getString("AREA_CODE")))).toMap
        val id2Name_AreaCodeBC: Broadcast[Map[String, (String, String)]] = ssc.sparkContext.broadcast(id2Name_AreaCode)

        val WithProvinceOrderInfoStream: DStream[OrderInfo] = modifiedWithIfConsumedOrderInfoStream.mapPartitions(
            orderInfoIter => {
                val orderInfoList: List[OrderInfo] = orderInfoIter.toList
                val map: Map[String, (String, String)] = id2Name_AreaCodeBC.value
                for (orderInfo <- orderInfoList) {
                    val province_id = orderInfo.province_id
                    orderInfo.province_name = map.getOrElse(province_id.toString, (null, null))._1
                    orderInfo.province_area_code = map.getOrElse(province_id.toString, (null, null))._2
                }
                orderInfoList.toIterator
            }
        )

        // TODO 7.混入用户维度

        val WithUserInfoOrderInfoStream: DStream[OrderInfo] = WithProvinceOrderInfoStream.transform(
            rdd => {
                val WithUserAgeGroupRdd: RDD[OrderInfo] = rdd.mapPartitions(
                    iter => {
                        val list: List[OrderInfo] = iter.toList
                        if(list.size != 0){
                            val uids: String = list.map("'" + _.user_id + "'").distinct.mkString(",")
                            val uid2UserInfo: Map[String, UserInfo] = PhoenixUtil.query("select * from ODS_USER_INFO where ID in (" + uids + ")", null)
                                    .map(jsonObj => (jsonObj.getString("ID"), jsonObj.toJavaObject(classOf[UserInfo])))
                                    .toMap
                            list.map(
                                orderInfo => {
                                    val userId = orderInfo.user_id
                                    val userInfo: UserInfo = uid2UserInfo.getOrElse(userId.toString, null)
                                    if (userInfo != null) {
                                        orderInfo.user_gender = userInfo.gender
                                        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
                                        val now: Date = new Date()
                                        val birthday = sdf.parse(userInfo.birthday)
                                        val age = (now.getTime - birthday.getTime) / 1000 / 60 / 60 / 24 / 365
                                        orderInfo.user_age_group = age match {
                                            case age if age < 18L => "小于18"
                                            case age if age < 28L => "18-28"
                                            case age if age < 45L => "28-45"
                                            case age if age < 60L => "45-60"
                                            case _ => "60以上"
                                        }
                                    }
                                    orderInfo
                                }
                            ).toIterator
                        }else{
                            iter
                        }
                    }
                )
                WithUserAgeGroupRdd
            }
        )

        // TODO 8.更新状态至HBase 保存ES 发送至Kafka DWD层 提交偏移量
        WithUserInfoOrderInfoStream.foreachRDD(
            rdd => {
                rdd.cache()

                // 更新状态至HBase
                rdd.filter(orderInfo => orderInfo.if_first_order == "1")
                        .map(info => UserState(info.user_id.toString,info.if_first_order))
                        .saveToPhoenix(
                            "USER_STATE",
                            Seq("USER_ID","IF_CONSUMED"),
                            new Configuration,
                            Some("spark105,spark106,spark107:2181")
                        )

                // 发送至Kafka DWD层
                rdd.foreach(
                    orderInfo => {
                        MyKafkaSink.sink(
                            "DWD_ORDER_INFO",
                            orderInfo.id.toString,
                            JSON.toJSONString(orderInfo,new SerializeConfig(true))
                        )
                    }
                )

                OffsetManager.saveOffsetRanges(topic,groupid,offsetRanges)
            }
        )



        ssc.start()
        ssc.awaitTermination()


    }

    case class UserState(id:String,if_consumed:String)

}
