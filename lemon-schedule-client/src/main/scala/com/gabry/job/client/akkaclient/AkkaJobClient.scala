package com.gabry.job.client.akkaclient

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import com.gabry.job.client.AbstractJobClient
import com.gabry.job.core.command.JobClientCommand
import com.gabry.job.core.domain.{Dependency, Job}
import com.gabry.job.core.event.{FailedEvent, JobTrackerEvent}
import com.gabry.job.core.registry.{Registry, RegistryFactory}
import com.typesafe.config.Config

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}
/**
  * Created by gabry on 2018/4/4 10:10
  * 默认客户端
  */
class AkkaJobClient(config:Config) extends AbstractJobClient(config) {

  private var started = false  //是否启动
  private val clusterName:String = config.getString("clusterNode.cluster-name")  //clusterName

  private var system:ActorSystem = _
  private var clientActor:ActorRef = _     //背后的Client Actor
  private var registry:Registry = _        //注册中心

  private val timeOutMs = config.getDuration("client.time-out").getSeconds
  private implicit val timeout: Timeout = Timeout(timeOutMs,TimeUnit.SECONDS)                //客户端提交Job的超时时间
  private implicit lazy val executionContext: ExecutionContextExecutor = system.dispatcher   //线程池，因为这里用的ask，这两个都是Future的隐式参数；

  //初始化注册中心；
  private def initRegistry():Unit = {
    registry = RegistryFactory.getRegistry(config).get
    registry.connect()
  }
  /**
    * 启动客户端
    */
  override def start(): Unit = {
    if(!started){
      try{
        initRegistry()  //初始化注册中心，
        system = ActorSystem(clusterName, config)  //创建ActorSystem
        clientActor = system.actorOf(Props.create(classOf[ClientActor],registry),"JobClient")  //创建 Client 子Actor，名字为JobClient
        clientActor ! JobClientCommand.Start       //发送启动JobClient的消息
        started = true
      }catch {
        case ex:Exception =>
          ex.printStackTrace()
      }
    }
  }

  /**
    * 提交作业
    * @param job 待提交的作业
    * @param dependency 作业的依赖
    */
  override def submitJob(job: Job,dependency: Array[Dependency]):Unit = {
    clientActor ? JobClientCommand.SubmitJob(job,dependency) onComplete{
      case Success(evt @ JobTrackerEvent.JobSubmitted(submittedJob)) =>
        println(s"[${evt.at}] 作业提交成功，提交后的作业是 $submittedJob")
      case Success(failedEvent:FailedEvent) =>
        println(s"[${failedEvent.at}] 作业提交失败，原因 ${failedEvent.reason}")
      case Success(unKnownMessage) =>
        println(s"客户端返回异常信息 $unKnownMessage")
      case Failure(reason:AskTimeoutException) =>
        println(s"超时了$reason")
      case Failure(reason) =>
        reason.printStackTrace()
    }
  }

  /**
    * 停止客户端
    */
  override def stop(): Unit = {
    if(started){
      registry.disConnect()  //断开注册中心连接
      clientActor ! JobClientCommand.Stop
      system.stop(clientActor) //杀死背后的ClientActor
      system.terminate()     //停止ActorSystem
      started = false
    }
  }

  /**
    * 当前客户端是否启动
    *
    * @return true启动
    */
  override def isStarted: Boolean = started

  /**
    * 取消作业执行 也是由Manager进行操作，然后返回；
    * @param jobId 待取消的作业ID
    */
  override def cancelJob(jobId: Long,force:Boolean): Unit = {
    clientActor ? JobClientCommand.CancelJob(jobId,force) onComplete{
      case Success(_) =>
        println("作业取消成功")
      case Failure(reason) =>
        reason.printStackTrace()
        println(s"作业取消失败 ${reason.getMessage}")
    }
  }
}
