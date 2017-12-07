package com.al.entity

import scala.beans.BeanProperty

class DataResult {
  @BeanProperty var prediction: Int = -1
  @BeanProperty var dimeid: Int = -1
  @BeanProperty var pv: Int = 0
  @BeanProperty var uv: Int = 0
  @BeanProperty var ip: Int = 0
}
