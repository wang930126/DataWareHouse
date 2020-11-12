package com.wang930126.app.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wang930126.bean.{BaseCategory3, UserInfo}
import com.wang930126.util.PropertiesUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object Dim_UserInfo {

    val topic = "ODS_T_USER_INFO"
    val groupid = "BASE_USER_INFO_GROUP"

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
            }
        )

        val UserInfoStream: DStream[UserInfo] = jsonObjStream.map(
            obj => UserInfo(
                obj.getString("id"),
                obj.getString("login_name"),
                obj.getString("nick_name"),
                obj.getString("passwd"),
                obj.getString("name"),
                obj.getString("phone_num"),
                obj.getString("email"),
                obj.getString("head_img"),
                obj.getString("user_level"),
                obj.getString("birthday"),
                obj.getString("gender"),
                obj.getString("create_time"),
                obj.getString("operate_time")
            )
        )

        UserInfoStream.print(10)

        import org.apache.phoenix.spark._
        UserInfoStream.foreachRDD(
            rdd => rdd.saveToPhoenix(
                "ODS_USER_INFO",
                //id varchar primary key,login_name varchar,nick_name varchar,passwd varchar,name varchar,phone_num varchar,email varchar,head_img varchar,user_level varchar,birthday varchar,gender varchar,create_time varchar,operate_time varchar
                Seq("ID","LOGIN_NAME","NICK_NAME","PASSWD","NAME","PHONE_NUM","EMAIL","HEAD_IMG","USER_LEVEL","BIRTHDAY","GENDER","CREATE_TIME","OPERATE_TIME"),//id varchar primary key,iso_code varchar,name varchar,area_code varchar,region_id varchar,region_name varchar
                new Configuration(),
                Some("spark105:2181")
            )
        )

        ssc.start()
        ssc.awaitTermination()

    }

}
