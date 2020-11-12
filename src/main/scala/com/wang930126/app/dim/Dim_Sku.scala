package com.wang930126.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.{ProvinceRegion, SkuInfo}
import com.wang930126.util.{PhoenixUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ListBuffer

object Dim_Sku {

    val topic = "ODS_T_SKU_INFO"
    val groupid = "BASE_SKU_GROUP"

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")

        val ssc: StreamingContext = new StreamingContext(conf,Seconds(3))

        val kafkaParams = Map[String,Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertiesUtil.getConfig("kafka.server"),
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
            ConsumerConfig.GROUP_ID_CONFIG -> groupid
        )

        val kafkaStream = KafkaUtils.createDirectStream[String,String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String,String](
                Seq(topic),
                kafkaParams
            )
        )

        val jsonObjStream: DStream[JSONObject] = kafkaStream.map(
            record => {
                val jsonStr: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                jsonObj
            }
        )
        // {"sku_desc":"迪奥（Dior）烈艳蓝金唇膏 口红 3.5g 999号 哑光-经典正红",
        // "tm_id":7,
        // "create_time":"2020-02-28 01:21:13",
        // "price":252,
        // "sku_default_img":"http://kAXllAQEzJWHwiExxVmyJIABfXyzbKwedeofMqwh",
        // "weight":0.00,
        // "sku_name":"迪奥（Dior）烈艳蓝金唇膏 口红 3.5g 999号 哑光-经典正红",
        // "id":15,
        // "spu_id":12,
        // "category3_id":477}
        //jsonObjStream.print(20)

        val tradeMark: Map[String, String] = PhoenixUtil.query("select * from ODS_BASE_TRADEMARK",null)
                .map(jsonObj => (jsonObj.getString("TM_ID"),jsonObj.getString("TM_NAME")))
                .toMap

        val category3: Map[String, (String, String)] = PhoenixUtil.query("select * from ODS_BASE_CATEGORY3", null)
                .map(jsonObj => (jsonObj.getString("ID"), (jsonObj.getString("NAME"), jsonObj.getString("CATEGORY2_ID"))))
                .toMap

        val category2: Map[String, (String, String)] = PhoenixUtil.query("select * from ODS_BASE_CATEGORY2", null)
                .map(jsonObj => (jsonObj.getString("ID"), (jsonObj.getString("NAME"), jsonObj.getString("CATEGORY1_ID"))))
                .toMap

        val category1: Map[String, String] = PhoenixUtil.query("select * from ODS_BASE_CATEGORY1", null)
                .map(jsonObj => (jsonObj.getString("ID"), jsonObj.getString("NAME")))
                .toMap

        val spu: Map[String, String] = PhoenixUtil.query("select * from ODS_SPU_INFO",null)
                        .map(jsonObj => (jsonObj.getString("ID"),jsonObj.getString("SPU_NAME")))
                        .toMap

        val SkuInfoStream: DStream[SkuInfo] = jsonObjStream.map(
            jsonObj => {
                val tm_id = jsonObj.getString("tm_id")
                val tm_name: String = tradeMark.getOrElse(tm_id, null)
                val category3_id = jsonObj.getString("category3_id")
                val category3_name: String = category3.getOrElse(category3_id, (null, null))._1
                val category2_id: String = category3.getOrElse(category3_id, (null, null))._2
                val category2_name = category2.getOrElse(category2_id, (null, null))._1
                val category1_id = category2.getOrElse(category2_id, (null, null))._2
                val category1_name = category1.getOrElse(category1_id, null)
                val spu_id = jsonObj.getString("spu_id")
                val spu_name = spu.getOrElse(spu_id, null)
                SkuInfo(
                    jsonObj.getString("id"),
                    spu_id,
                    jsonObj.getDouble("price"),
                    jsonObj.getString("sku_name"),
                    jsonObj.getString("sku_desc"),
                    jsonObj.getDouble("weight"),
                    tm_id,
                    tm_name,
                    category3_id,
                    category2_id,
                    category1_id,
                    category3_name,
                    category2_name,
                    category1_name,
                    spu_name,
                    jsonObj.getString("create_time")
                )
            }
        )

        SkuInfoStream.print(10)

        import org.apache.phoenix.spark._
        SkuInfoStream.foreachRDD(
            rdd => rdd.saveToPhoenix(
                "DWD_SKU_INFO",
                //id varchar primary key,spu_id varchar,price Double,sku_name varchar,sku_desc varchar,weight double,tm_id varchar,tm_name varchar,category3_id varchar,category2_id varchar,category1_id varchar,
                //category3_name varchar,category2_name varchar,category1_name varchar,spu_name varchar,create_time varchar
                Seq("ID","SPU_ID","PRICE","SKU_NAME","SKU_DESC","WEIGHT","TM_ID","TM_NAME",
                    "CATEGORY3_ID","CATEGORY2_ID","CATEGORY1_ID", "CATEGORY3_NAME","CATEGORY2_NAME",
                    "CATEGORY1_NAME","SPU_NAME","CREATE_TIME"),//id varchar primary key,iso_code varchar,name varchar,area_code varchar,region_id varchar,region_name varchar
                new Configuration(),
                Some("spark105:2181")
            )
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
