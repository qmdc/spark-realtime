package com.qiandao.gmall.realtime.util

import java.util.ResourceBundle

/**
 * 配置文件解析类
 */
object MyPropsUtils {

  private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

  def apply(propsKey: String) : String ={
    bundle.getString(propsKey)
  }

  //获取资源文件测试
  def main(args: Array[String]): Unit = {
    println(MyPropsUtils("kafka.bootstrap-servers"))
  }
}
