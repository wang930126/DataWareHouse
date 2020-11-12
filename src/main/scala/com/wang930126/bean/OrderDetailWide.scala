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
                          ){

    def this(info:OrderInfo,detail:OrderDetail){
        this
        this.order_detail_id = detail.id
        this.order_id = info.id
        this.order_status = info.order_status
        this.create_time = info.create_time
        this.user_id = info.user_id
        this.sku_id = detail.sku_id
        this.sku_price = detail.sku_price
        this.sku_num = detail.sku_num
        this.sku_name = detail.sku_name
        this.benefit_reduce_amount = info.benefit_reduce_amount
        this.original_total_amount = info.original_total_amount
        this.feight_fee = info.feight_fee
        this.final_total_amount = info.final_total_amount
        this.if_first_order = info.if_first_order
        this.province_name = info.province_name
        this.province_area_code = info.province_area_code
        this.user_age_group = info.user_age_group
        this.user_gender = info.user_gender
        this.dt = info.create_date
        this.spu_id = detail.spu_id
        this.tm_id = detail.tm_id
        this.category3_id = detail.category3_id
        this.category3_name = detail.category3_name
        this.spu_name = detail.spu_name
        this.tm_name = detail.tm_name
    }

}

