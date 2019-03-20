package com.utils

object LocationUtil {
  def isYsrequset(requestmode:Int, processnode:Int): Int ={
    if(requestmode == 1 && processnode >= 1)
      return 1
    return 0
  }

  def isYxrequset(requestmode:Int, processnode:Int): Int ={
    if(requestmode == 1 && processnode >= 2)
          return 1
    return 0
  }
  def isAdrequest(requestmode:Int, processnode:Int): Int ={
    if(requestmode == 1 && processnode == 3)
      return 1
    return 0
  }

  def isCybid(iseffective:Int, isbilling:Int, isbid:Int): Int ={
    if(iseffective==1 && isbilling==1 &&isbid==1)
      return 1
    return 0
  }

  def isCybidsuccees(iseffective:Int, isbilling:Int, isbid:Int,adorderid:Int): Int = {
    if (iseffective == 1 && isbilling == 1 && isbid == 1&&adorderid!=0)
      return 1
    return 0
  }

  def isShows(requestmode:Int, iseffective:Int): Int ={
    if(requestmode==2 && iseffective == 1)
      return 1
    return 0
  }

  def isClicks(requestmode:Int, iseffective:Int): Int ={
    if(requestmode==3 && iseffective == 1)
      return 1
    return 0
  }

  def isDspcost(requestmode:Int, iseffective:Int, winprice:Double): Double ={
    if(requestmode==1 && iseffective == 1)
      return winprice/1000
    return 0
  }
  def isDspapy(requestmode:Int, iseffective:Int,adpayment:Double): Double ={
    if(requestmode==1 && iseffective == 1)
      return adpayment/1000
    return 0
  }
}
