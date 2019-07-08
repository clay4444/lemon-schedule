package com.gabry.job.manager.daemon

import com.gabry.job.core.daemon.DaemonCreator
import com.gabry.job.manager.node.JobTrackerNode

import scala.util.{Failure, Success}


/**
  * Created by gabry on 2018/3/29 10:42
  * 启动JobManager节点的主类
  */
object JobManagerDaemon {

  def main(args: Array[String]): Unit = {
    val port = args.headOption.map(_.toInt)

    DaemonCreator.createDaemon(JobTrackerNode,port) match {
      case Success(daemon) =>
        println(s"JobTrackerNode start at [${daemon.actor}]")
      case Failure(reason) =>
        println(s"JobTrackerNode start failed ,reason: $reason")
        reason.printStackTrace()
    }
  }

}
