package com.wang930126.bean

case class OrderDetail(
                              id:Long,
                              order_id:Long,
                              sku_id:Long,
                              var sku_price:Double,
                              sku_num:Long,
                              sku_name:String,
                              var spu_id:Long,
                              var tm_id:Long,
                              var category3_id:Long,
                              var spu_name:String,
                              var tm_name:String,
                              var category3_name:String
                      )
