package com.tag

trait Tags {
  /**
    * 定义一个打标签的接口
    */
  def makeTags(arg:Any*):List[(String, Int)]
}
