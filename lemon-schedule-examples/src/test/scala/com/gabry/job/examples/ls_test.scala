package com.gabry.job.examples

object ls_test {

  def main(args: Array[String]): Unit = {

    val list = List(1,2,3,4)
    val list_f = list.filter(_ == 3);

    list_f.foreach(println)
  }
}
