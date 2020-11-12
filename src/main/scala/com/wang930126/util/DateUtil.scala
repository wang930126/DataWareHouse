package com.wang930126.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

    def main(args:Array[String]) = {

        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val birthday_str = "1993-01-26"
        val birthday = sdf.parse(birthday_str)
        val now = new Date()

        val timeDiff: Long = now.getTime - birthday.getTime

        println(timeDiff/1000/60/60/24/365)

    }

}
