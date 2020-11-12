package com.wang930126.util

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}

object JsonUtil {

    def isValidateJson(jsonStr:String) = {
        try{
            val jsonObj: JSONObject = JSON.parseObject(jsonStr)
            if(jsonObj.containsKey("mid") && jsonObj.containsKey("ts")){
                true
            }else{
                false
            }
        }catch{
            case e:JSONException => false
        }
    }

}
