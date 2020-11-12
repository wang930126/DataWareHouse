package com.wang930126.bean

case class OrderDetailWide(
                                  var order_detail_id:Long =0L, //order_detail
                                  var order_id: Long=0L, //order_info
                                  var order_status:String=null, //order_info
                                  var create_time:String=null, //order_info
                                  var user_id: Long=0L, //order_info
                                  var sku_id: Long=0L, //order_detail
                                  var sku_price: Double=0D, //order_detail
                                  var sku_num: Long=0L, //order_detail
                                  var sku_name: String=null, //order_detail
                                  var benefit_reduce_amount:Double =0D , //order_info
                                  var original_total_amount:Double =0D , //order_info
                                  var feight_fee:Double=0D, //order_info
                                  var final_total_amount: Double =0D , //order_ifo
                                  var final_detail_amount:Double=0D, //order_detail

                                  var if_first_order:String=null, //order_info

                                  var province_name:String=null, //order_info
                                  var province_area_code:String=null, //order_info

                                  var user_age_group:String=null, //order_info
                                  var user_gender:String=null, //order_info

                                  var dt:String=null, //order_info

                                  var spu_id: Long=0L, //order_detail
                                  var tm_id: Long=0L, //order_detail
                                  var category3_id: Long=0L, //order_detail
                                  var spu_name: String=null, //order_detail
                                  var tm_name: String=null, //order_detail
                                  var category3_name: String=null //order_detail
                          )

