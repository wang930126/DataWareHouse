package com.wang930126.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.{BaseCategory3, ProvinceRegion}
import com.wang930126.util.{PhoenixUtil, PropertiesUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object ODS_BASE_Category3 {

    val topic = "ODS_T_BASE_CATEGORY3"
    val groupid = "BASE_CATEGORY3_GROUP"

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
                val jsonObj= JSON.parseObject(jsonStr)
                jsonObj
                //PRINT(jsonObj)
                //{"name":"京东卡","id":759,"category2_id":76}
            }
        )

        val BaseCategory3Stream: DStream[BaseCategory3] = jsonObjStream.map(
            obj => BaseCategory3(
                obj.getString("id"),
                obj.getString("name"),
                obj.getString("category2_id")
            )
        )

        import org.apache.phoenix.spark._
        BaseCategory3Stream.foreachRDD(
            rdd => rdd.saveToPhoenix(
                "ODS_BASE_CATEGORY3",
                Seq("ID","NAME","CATEGORY2_ID"),//id varchar primary key,iso_code varchar,name varchar,area_code varchar,region_id varchar,region_name varchar
                new Configuration(),
                Some("spark105:2181")
            )
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
