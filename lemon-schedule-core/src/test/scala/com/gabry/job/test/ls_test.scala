package com.gabry.job.test

import com.alibaba.fastjson.{JSON, JSONObject}


object ls_test {

  implicit class JsonHelper(val sc:StringContext) extends AnyVal{
    def json(args:Any*):JSONObject={
      val strings=sc.parts.iterator
      val expressions=args.iterator
      var buf=new StringBuffer(strings.next)
      while(strings.hasNext){
        val exp = expressions.next
        val str = strings.next

        println("==" + exp)
        println("--" + str)

        buf append exp
        buf append str
      }
      println("buf: " + buf.toString)
      JSON.parseObject(buf.toString)
    }
  }

  def main(args: Array[String]): Unit = {

    val name="James"
    val id = 1
    println(s"Hello,$name")//Hello,James

    //id"string content" 它都会被转换成一个StringContext实例的call(id)方法。
    //这个方法在隐式范围内仍可用。只需要简单得建立一个隐类，给StringContext实例增加一个新方法，便可以定义我们自己的字符串插值器。如下例：


    def giveMeSomeJson(x:JSONObject) = {
      println("!!" + x.toJSONString)
    }

    //在这个例子中，我们试图通过字符串插值生成一个JSON文本语法。隐类 JsonHelper 作用域内使用该语法，且这个JSON方法需要一个完整的实现。只不过，字符串字面值格式化的结果不是一个字符串，而是一个JSON对象。
    giveMeSomeJson(json"{name:$name,id:$id}")


    //当编译器遇到”{name:$name,id:$id”}”，它将会被重写成如下表达式：
    //new StringContext("{name:",",id:","}").json(name,id)

    //隐类则被重写成如下形式
    //new JsonHelper(new StringContext("{name:",",id:","}")).json(name,id)

  }
}
