package com.wang930126.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, ResultSetMetaData}

import com.alibaba.fastjson.JSONObject

import scala.collection.mutable.ListBuffer

object PhoenixUtil {

    def query(sql:String,args:Array[String]) = {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
        val lb: ListBuffer[JSONObject] = ListBuffer[JSONObject]()
        var conn: Connection = null
        var pst:PreparedStatement = null
        var rs:ResultSet = null
        try{
            conn = DriverManager.getConnection(PropertiesUtil.getConfig("phoenix.hbase.zkServer.url"))
            pst = conn.prepareStatement(sql)
            if(args !=null && args.length != 0){
                for(index <- 1 to args.length - 1){
                    pst.setObject(index,args(index - 1))
                }
            }
            rs = pst.executeQuery()
            val metaData: ResultSetMetaData = rs.getMetaData

            while(rs.next()){
                val jsonObj: JSONObject = new JSONObject()
                for(index <- 1 to metaData.getColumnCount){
                    jsonObj.put(metaData.getColumnName(index),rs.getObject(index))
                }
                lb.append(jsonObj)
            }
        }catch{
            case e:Exception => e.printStackTrace()
        }finally{
            if(rs != null) rs.close()
            if(pst != null) pst.close()
            if(conn != null) conn.close
        }
        lb
    }

    def main(args:Array[String]) = {
//        val uids = ListBuffer(1,2,3,4).map("'" + _ + "'").mkString(",")
//        val sql:String = "select * from DWD_SKU_INFO where ID in (" + uids + ")"
//        println(PhoenixUtil.query(sql, null))

        val list = List(1,3,2,4)
        list.sortBy(a=>a)
        println(list)
    }

}
