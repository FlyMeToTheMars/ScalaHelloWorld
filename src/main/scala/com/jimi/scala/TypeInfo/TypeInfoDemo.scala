package com.jimi.scala.TypeInfo

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.PojoTypeInfo

/**
  * @Author Administrator
  * @create 2019/8/26 11:13
  */
object TypeInfoDemo {
  case class TypeDemo(
                     `type`: String,
                     demo: String
                     )

  def main(args: Array[String]): Unit = {
    val Type1: TypeDemo = TypeDemo("1","2")
  }

}
