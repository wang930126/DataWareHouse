package com.wang930126.app.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.wang930126.bean.{OrderDetail, SkuInfo}
import com.wang930126.util.{MyKafkaSink, OffsetManager, PhoenixUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * 用于将ODS层发来的ODS_T_ORDER_DETAIL的数据进行解析 并整合维度字段 最终写入DWD层
  */
object OrderDetailApp {

    val topic = "ODS_T_ORDER_DETAIL"
    val groupid = "ODS_ORDER_DETAIL_GROUP"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        conf.set("spark.streaming.stopGracefullyOnShutdown","true")
        conf.set("spark.streaming.kafka.maxRatePerPartition","500")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        // TODO 1.从redis中获取offsetMap
        val offsetMap: mutable.Map[TopicPartition, Long] = OffsetManager.getOffset(groupid,topic)

        // TODO 2.创建kafka流
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


        // TODO 3.获取偏移量 流转成样例类对象
        var offsetRanges = Array[OffsetRange]()
        val orderDetailStream: DStream[OrderDetail] = kafkaStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                val orderDetailRdd: RDD[OrderDetail] = rdd.map {
                    case record => JSON.parseObject(record.value(), classOf[OrderDetail])
                }
                orderDetailRdd
            }
        )

        //需要关联 sku表
        //orderDetailStream.print(10)
        //OrderDetail(0,3907,6,0.0,1,北纯精制黄小米（小黄米 月子米 小米粥 粗粮杂粮 大米伴侣）2.18kg,0.0,0,0,0,null,null,null)
        //"sku_num":"2",
        // "create_time":"2020-11-07 23:41:31",
        // "img_url":"http://IJwhvJSuwaMHaevQQKZUzLkOsvYFiYsRhmLSjpZb",
        // "sku_id":7,
        // "sku_name":"荣耀10青春版 幻彩渐变 2400万AI自拍 全网通版4GB+64GB 渐变蓝 移动联通电信4G全面屏手机 双卡双待",
        // "order_price":1553.00,
        // "source_type":"2401",
        // "id":9442,
        // "order_id":3901

        orderDetailStream.cache()

        val orderDetailWithSkuInfoStream: DStream[OrderDetail] = orderDetailStream.mapPartitions(
            orderDetailItr => {
                // 判断这个分区中是否有数据 如果有 执行查询HBase 如果没有直接返回这个迭代器
                val orderDetailList: List[OrderDetail] = orderDetailItr.toList
                if (orderDetailList.size > 0) {
                    val sku_ids = orderDetailList.map("'" + _.sku_id + "'").mkString(",")
                    val sql = "select * from DWD_SKU_INFO where ID in (" + sku_ids + ")"
                    val id2SkuInfo: Map[String, SkuInfo] = PhoenixUtil.query(sql, null)
                            .map(jsonObj => (jsonObj.getString("ID"), jsonObj.toJavaObject(classOf[SkuInfo])))
                            .toMap
                    for (orderDetail <- orderDetailList) {
                        val sku_info = id2SkuInfo.getOrElse(orderDetail.sku_id.toString, null)
                        if (sku_info != null) {
                            orderDetail.sku_price = sku_info.price
                            orderDetail.spu_id = sku_info.spu_id.toLong
                            orderDetail.tm_id = sku_info.tm_id.toLong
                            orderDetail.category3_id = sku_info.category3_id.toLong
                            orderDetail.spu_name = sku_info.spu_name
                            orderDetail.tm_name = sku_info.tm_name
                            orderDetail.category3_name = sku_info.category3_name
                        }
                    }
                    orderDetailList.toIterator
                } else {
                    orderDetailItr
                }
            }
        )

        orderDetailWithSkuInfoStream.cache()
        //orderDetailWithSkuInfoStream.print(10)

        orderDetailWithSkuInfoStream.foreachRDD(
            rdd => {
                // 发送到DWD层Kafka
                rdd.foreach(
                    orderDetail => {
                        MyKafkaSink.sink(
                            "DWD_ORDER_DETAIL",
                            orderDetail.id.toString,
                            JSON.toJSONString(orderDetail,new SerializeConfig(true)))
                    }
                )

                // 提交偏移量
                OffsetManager.saveOffsetRanges(topic,groupid,offsetRanges)
            }
        )

        ssc.start()
        ssc.awaitTermination()

    }
}
