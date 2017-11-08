package com.al.entity

import scala.beans.BeanProperty

/**
  * Created by An on 2016/11/25.
  */
class Content extends Serializable {
  @BeanProperty var contentId: Long = 0
  @BeanProperty var url: String = ""
  @BeanProperty var title: String = ""
  @BeanProperty var day: String = ""
  @BeanProperty var pv: Int = 0
  @BeanProperty var uv: Int = 0

  @BeanProperty var dimeId: Int = 0
  @BeanProperty var second: Int = 0

  var uvs = Set[String]()

  def this(pv: Int, uv: Int){
    this()
    this.pv = pv
    this.uv = uv
  }
}
