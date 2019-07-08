package com.gabry.job.test

import com.alibaba.fastjson.{JSON, JSONObject}

object ls_test2 {

  implicit class JsonHelper(val sc:StringContext) extends AnyVal{
    def l(args:Any*):String={
      val expressions=args.iterator

      val mid = expressions.next.toString
      println(mid)
      s"$${mid}"
    }
  }

  def main(args: Array[String]): Unit = {

    val last_pt = 20190707
    val origin = "abc${last_pt}"

    println(s"${origin}")
  }
}
