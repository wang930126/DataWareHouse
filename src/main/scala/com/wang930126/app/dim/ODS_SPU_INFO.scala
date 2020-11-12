package com.wang930126.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.{BaseCategory3, SpuInfo}
import com.wang930126.util.PropertiesUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object ODS_SPU_INFO {

    val topic = "ODS_T_SPU_INFO"
    val groupid = "BASE_SPU_INFO_GROUP"

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

        val SpuInfoStream: DStream[SpuInfo] = jsonObjStream.map(
            obj => SpuInfo(
                obj.getString("id"),
                obj.getString("spu_name"),
                obj.getString("description"),
                obj.getString("category3_id"),
                obj.getString("tm_id")
            )
        )

        SpuInfoStream.print(10)

        import org.apache.phoenix.spark._
        SpuInfoStream.foreachRDD(
            rdd => rdd.saveToPhoenix(
                "ODS_SPU_INFO",
                Seq("ID","SPU_NAME","DESCRIPTION","CATEGORY3_ID","TM_ID"),//id varchar primary key,iso_code varchar,name varchar,area_code varchar,region_id varchar,region_name varchar
                new Configuration(),
                Some("spark105:2181")
            )
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
