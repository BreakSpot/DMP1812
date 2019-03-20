package com.utils

object AppNameUtil {
  def getName(app1:String, app2: Array[(String, String)] ): String ={
    for(arr <- app2){
      if(arr._1.equals(app1))
        return arr._2
    }
    return null
  }
}
