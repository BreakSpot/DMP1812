package com.utils

object LocationUtilV2 {

  //处理请求数
  def requestUtil(reqMode:Int, prcMode:Int):List[Double]={
    if(reqMode== 1&& prcMode==1){
      List[Double](1,0,0)
    } else if(reqMode== 1 && prcMode==2){
      List[Double](1,1,0)
    }else if(reqMode==1 && prcMode==3){
      List[Double](1,1,1)
    }else
      List[Double](0,0,0)
  }
  // 处理竞价和广告成本消费
  def requstAD(fective:Int, bill:Int, bid:Int,
               win:Int, ad:Int, winPrice:Double, adPayMent:Double): List[Double] ={
    if(fective==1 && bill==1 && bid == 1){
      List[Double](1,0,0,0)
    }
    else if(fective==1 && bill==1 && win == 1){
      if(ad != 0){
        List[Double](0,1,winPrice/1000,adPayMent/1000)
      }else{
        List[Double](0,0,winPrice/1000,adPayMent/1000)
      }
    }else
      List[Double](0,0,0,0)
  }
  //展示，点击
  def rquestShow(reqMode:Int, fective:Int):List[Double]={
    if(reqMode == 2 && fective == 1){
      List[Double](1,0)
    }else if(reqMode == 3 && fective == 1) {
      List[Double](0, 1)
    }else
      List[Double](0,0)
  }

}
