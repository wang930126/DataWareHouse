package com.wang930126.util

import java.util.Properties

object PropertiesUtil {

    val prop = new Properties()
    val path = "config.properties"
    prop.load(PropertiesUtil.getClass.getClassLoader.getResourceAsStream(path))

    def getConfig(s:String) = prop.get(s).toString

}
