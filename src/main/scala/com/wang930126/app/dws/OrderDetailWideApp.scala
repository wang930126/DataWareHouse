package com.wang930126.app.dws

import java.{lang, util}
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.wang930126.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.wang930126.util.{MyKafkaSink, OffsetManager, PropertiesUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * 订阅kafka dwd层 将两条流进行join 计算价格均摊 保存入clickhouse 打入 kafka dws层
  */
object OrderDetailWideApp {

    val orderInfoTopic = "DWD_ORDER_INFO"
    val orderDetailTopic = "DWD_ORDER_DETAIL"
    val orderDetailWideGroup = "DWD_ORDER_DETAIL_WIDE_GROUP"
    val batchDuration = 3

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        conf.set("spark.streaming.stopGracefullyOnShutdown","true")
        conf.set("spark.streaming.kafka.maxRatePerPartition","500")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(batchDuration))

        // TODO 1.从redis中查询偏移量
        val offsetMapOrderInfo: mutable.Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailWideGroup,orderInfoTopic)
        val offsetMapOrderDetail: mutable.Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailWideGroup,orderDetailTopic)

        val kafkaParams = Map[String,Object](
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getConfig("kafka.server"),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
            ConsumerConfig.GROUP_ID_CONFIG -> orderDetailWideGroup,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
        )

        // TODO 2.根据偏移量创建kafka流
        val orderInfoKafkaStream = if(offsetMapOrderInfo.isEmpty){
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(orderInfoTopic),
                    kafkaParams
                )
            )
        }else{
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(orderInfoTopic),
                    kafkaParams,
                    offsetMapOrderInfo
                )
            )
        }

        val orderDetailKafkaStream = if(offsetMapOrderDetail.isEmpty){
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(orderDetailTopic),
                    kafkaParams
                )
            )
        }else{
            KafkaUtils.createDirectStream[String,String](
                ssc,
                LocationStrategies.PreferConsistent,
                ConsumerStrategies.Subscribe[String,String](
                    Seq(orderDetailTopic),
                    kafkaParams,
                    offsetMapOrderInfo
                )
            )
        }

        // TODO 3.两条流分别获取偏移量 并 转为样例类
        var offsetRangeOrderInfo = Array[OffsetRange]()
        var offsetRangeOrderDetail = Array[OffsetRange]()

        val orderInfoStream: DStream[(Long, OrderInfo)] = orderInfoKafkaStream.transform(
            rdd => {
                offsetRangeOrderInfo = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                val id2OrderInfoRDD: RDD[(Long, OrderInfo)] = rdd.map(
                    record => {
                        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
                        (orderInfo.id, orderInfo)
                    }
                )
                id2OrderInfoRDD
            }
        )

        orderInfoStream.cache()

        val orderDetailStream: DStream[(Long,OrderDetail)] = orderDetailKafkaStream.transform(
            rdd => {
                offsetRangeOrderDetail = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                val orderDetailRDD: RDD[(Long,OrderDetail)] = rdd.map(
                    record => {
                        val orderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
                        (orderDetail.order_id,orderDetail)
                    }
                )
                orderDetailRDD
            }
        )

        orderDetailStream.cache()

        // TODO 4.两条流分别开窗 然后join
        val orderInfoStreamWithWindow = orderInfoStream.window(Seconds(batchDuration * 2),Seconds(batchDuration))
        val orderDetailStreamWithWindow = orderDetailStream.window(Seconds(batchDuration * 2),Seconds(batchDuration))

        val orderDetailWideStream: DStream[OrderDetailWide] = orderInfoStreamWithWindow.join(orderDetailStreamWithWindow)
                .map {
                    case (orderId, (orderInfo: OrderInfo, orderDetail: OrderDetail)) => {
                        new OrderDetailWide(orderInfo, orderDetail)
                    }
                }

        orderDetailWideStream.cache()

        // TODO 5.利用redis过滤已经join上的订单详单
        // TODO 6.将过滤后的join上的结果保存到redis中

        val filteredStream: DStream[OrderDetailWide] = orderDetailWideStream.transform(
            rdd => {
                val filteredRDD: RDD[OrderDetailWide] = rdd.mapPartitions(
                    orderDetailWideItr => {
                        val wideList: List[OrderDetailWide] = orderDetailWideItr.toList
                        if (wideList.size > 0) {
                            val lb = ListBuffer[OrderDetailWide]()
                            val conn = RedisUtil.getResource()
                            try {
                                val sdf = new SimpleDateFormat("yyyy-MM-dd")
                                for (wide <- wideList) {
                                    val key = "order_detail_" + wide.dt
                                    val res: lang.Long = conn.sadd(key, wide.order_detail_id.toString)
                                    if (res == 1L) {
                                        lb.append(wide)
                                    }
                                }
                            } catch {
                                case e: Exception => e.printStackTrace()
                            } finally {
                                RedisUtil.returnResource(conn)
                            }
                            lb.toIterator
                        } else {
                            orderDetailWideItr
                        }
                    }
                )
                filteredRDD
            }
        )

        // TODO 7.计算价格均摊
        val orderDetailWideWithDetailAmountStream: DStream[OrderDetailWide] = filteredStream.mapPartitions(
            iter => {
                val list: List[OrderDetailWide] = iter.toList
                if (list.size > 0) {
                    val conn = RedisUtil.getResource()
                    val originKey = "order_wide_origin_key"
                    val factKey = "order_wide_fact_key"
                    try {
                        for (wide <- list) {
                            // 从redis中取出 某个订单id下 已经计算出来的详单的和
                            val orderId2OriginSum = conn.hgetAll(originKey)
                            val orderId2FactSum = conn.hgetAll(factKey)

                            val skuNum_skuPrice = wide.sku_num * wide.sku_price
                            val orderId = wide.order_id

                            val originSum: Double = orderId2OriginSum.getOrElse(orderId.toString, "0.0").toDouble
                            val factSum: Double = orderId2FactSum.getOrElse(orderId.toString, "0.0").toDouble

                            // 如果这张详单 恰好是 订单的最后一笔 采用final_total_amount - factSum 计算
                            if (wide.original_total_amount - skuNum_skuPrice == originSum) {
                                wide.final_detail_amount = wide.final_total_amount - factSum
                                // 如果这张详单不是 最后的订单
                            } else {
                                wide.final_detail_amount = skuNum_skuPrice / wide.original_total_amount * wide.final_total_amount
                            }

                            // 更新状态回去
                            orderId2OriginSum.put(orderId.toString, (originSum + skuNum_skuPrice).toString)
                            conn.hmset(originKey, orderId2OriginSum)
                            orderId2FactSum.put(orderId.toString, (factSum + wide.final_detail_amount).toString)
                            conn.hmset(factKey, orderId2FactSum)

                        }
                    } catch {
                        case e: Exception => e.printStackTrace()
                    } finally {
                        RedisUtil.returnResource(conn)
                    }
                }
                list.toIterator
            }
        )

        orderDetailWideWithDetailAmountStream.cache()
        orderDetailWideWithDetailAmountStream.print()
        // TODO 8.写入clickhouse 写入kafka dws层 提交偏移量

        orderDetailWideWithDetailAmountStream.foreachRDD(
            rdd =>{

                //写入clickhouse


                //写入kafka_dws层
                rdd.foreach(
                    wide => MyKafkaSink.sink(
                        "DWS_ORDER_DETAIL_WIDE",
                        wide.order_detail_id.toString,
                        JSON.toJSONString(wide,new SerializeConfig(true))
                    )
                )

                //提交偏移量
                OffsetManager.saveOffsetRanges(orderInfoTopic,orderDetailWideGroup,offsetRangeOrderInfo)
                OffsetManager.saveOffsetRanges(orderDetailTopic,orderDetailWideGroup,offsetRangeOrderDetail)

            }
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
