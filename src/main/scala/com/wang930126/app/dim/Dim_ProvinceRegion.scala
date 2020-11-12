package com.wang930126.app.dim


import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.ProvinceRegion
import com.wang930126.util.{PhoenixUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object Dim_ProvinceRegion {

    val topic = "ODS_T_BASE_PROVINCE"
    val groupid = "BASE_PROVINCE_GROUP"

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

        val idToRegionName: Map[String, String] = PhoenixUtil.query("select * from ODS_BASE_REGION", null).map(
            obj => (obj.getString("ID"), obj.getString("REGION_NAME"))
        ).toMap

        println(idToRegionName)

        val ProvinceRegionStream: DStream[ProvinceRegion] = kafkaStream.map(
            record => {
                val jsonStr: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonStr)

                //println(jsonObj)
                //{"area_code":"320000","name":"江苏","region_id":"2","id":7,"iso_code":"CN-32"}

                //case class ProvinceRegion(id:String,
                //                          iso_code:String,
                //                          name:String,
                //                          area_code:String,
                //                          region_id:String,
                //                          region_name:String
                //                         )
                val region_id: String = jsonObj.getString("region_id")
                ProvinceRegion(jsonObj.getString("id"),
                    jsonObj.getString("iso_code"),
                    jsonObj.getString("name"),
                    jsonObj.getString("area_code"),
                    jsonObj.getString("region_id"),
                    idToRegionName.getOrElse(region_id, null)
                )
            }
        )

        import org.apache.phoenix.spark._
        ProvinceRegionStream.foreachRDD(
            rdd => rdd.saveToPhoenix(
                "ODS_BASE_PROVINCE",
                Seq("ID","ISO_CODE","NAME","AREA_CODE","REGION_ID","REGION_NAME"),//id varchar primary key,iso_code varchar,name varchar,area_code varchar,region_id varchar,region_name varchar
                new Configuration(),
                Some("spark105:2181")
            )
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
