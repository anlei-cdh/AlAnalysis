package com.al.entity

import com.al.util.StringUtil

import scala.beans.BeanProperty

/**
  * Created by An on 2016/11/24.
  */
class Log {
  @BeanProperty var Ts: Long = 0L
  @BeanProperty var Ip: String = ""
  @BeanProperty var Uuid: String = ""

  @BeanProperty var SearchEngine: String = ""
  @BeanProperty var Country: String = ""
  @BeanProperty var Area: String = ""

  @BeanProperty var ContentId: Long = 0L
  @BeanProperty var Url: String = ""
  @BeanProperty var Title: String = ""

  @BeanProperty var wd: Wd = null

  def getPagetype(): Char = {
    if(wd != null && wd.getT() != null) {
      val wdt: String  = wd.getT()
      if(wdt.length() == 3) {
        val pt: Char = wdt.charAt(2)
        if(pt >= '0' && pt <= '5') {
          return pt
        }
      }
    }
    return '6'
  }

  def getClearTitle(): String = {
    var title = ""
    if(Title != null) {
      title = StringUtil.clearTitleAll(Title)
    }
    return title
  }

  /**
    * 日志是否合法
    * @return
    */
  def isLegal(): Boolean = {
    if(Ip == null || Ip.equals("") || Uuid == null || Uuid.equals("")) {
      return false
    }
    return true
  }
}

class Wd {
  @BeanProperty var t: String = ""
}
