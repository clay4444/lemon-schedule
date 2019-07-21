package com.gabry.job.core.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Created by gabry on 2018/5/15 10:43
  */

object AutoSpeedActor{

  final case class BatchMessage(sourceMessage:Option[Any],lastMessageTime:Long, commit:Boolean = false) //消息的封装
  private[actor] trait InternalMessage{   //内部消息
    def systemTimestamp:Long
  }
  private[actor] final case class EnterBatch(systemTimestamp:Long) extends InternalMessage
  private[actor] final case class LeaveBatch(systemTimestamp:Long) extends InternalMessage
  private[actor] final case class BatchInterval(systemTimestamp:Long) extends InternalMessage
}

/**
  * @param batchNumber 自适应时批量的数量
  * @param batchInterval 自适应时批量的时间间隔
  */
abstract class AutoSpeedActor(batchNumber:Long,batchInterval:Duration,startTime:Long) extends SimpleActor {
  /**
    * 时间守卫。
    * 用来在批量模式下，及时提交当前剩余批量消息
    */
  private var timerGuard: ActorRef = _
  /**
    * 当前批量开始时间
    */
  private var batchStartTimestamp:Long = startTime
  /**
    * 当前批量结束时间
    */
  private var batchEndTimestamp:Long = batchStartTimestamp
  /**
    * 批量计数器
    */
  private var batchCounter:Long = 0

  /**
    * 获取当前消息的时间戳
    * @param msg 当前消息
    * @return 当前消息的时间戳
    */
  def getMessageTimestamp(msg: Any):Long

  /**
    * 判断当前消息是否自动驾驶，
    * @param msg 当前消息
    * @return true则对此类型的消息自动调整速率
    */
  def isAutoDriveMessage(msg:Any):Boolean

  /**
    * 判断当前是否为内部消息
    * @param msg 当前消息
    * @return true表示当前消息为内部消息
    */
  private def isIntervalMessage(msg:Any):Boolean = msg.isInstanceOf[AutoSpeedActor.InternalMessage]

  override def preStart(): Unit = {
    super.preStart()
    timerGuard = context.actorOf(Props.create(classOf[AutoSpeedActorGuard],batchInterval),self.path.name+"timerGuard")
    log.debug(s"AutoSpeedActor started for ${context.parent.path.name} batchNumber is $batchNumber,batchInterval is $batchInterval")
  }

  override def postStop(): Unit = {
    super.postStop()
    context.stop(timerGuard)
  }
  /**
    * 消息拦截器，初始化为单条模式
    */
  private var messageIntercept: (Any) => Any = singleProcess

  /**
    * 批量模式下，封装当前消息
    * @param currentMsg 当前消息
    * @return 封装后的批量消息
    */
  private def batchProcess(currentMsg:Any):Any = currentMsg match {
      case AutoSpeedActor.BatchInterval(systemTimestamp) =>   //时间守卫给它发的，代表超时时间
        log.debug(s"Receive AutoSpeedActor.BatchInterval message at $systemTimestamp ")
        if( batchCounter < batchNumber ){ //当前这个批次处理的消息过少，则退出批量模式，
          timerGuard ! AutoSpeedActor.LeaveBatch(System.currentTimeMillis())
          messageIntercept = singleProcess
        }
        //收到超时时间时，直接先提交一个批次，这样做是因为下一次超时消息到来时，确保batchCounter是从0加上去的，否则上面内个判断下一个批次是否处理消息过少的条件就不准确了，
        batchCounter = 0
        batchStartTimestamp = batchEndTimestamp
        AutoSpeedActor.BatchMessage(None,batchEndTimestamp,commit = true)
      case _ =>     //真实的消息
        batchEndTimestamp = getMessageTimestamp(currentMsg) //更新结束时间
        val commit = batchCounter % batchNumber == 0       //判断当前批次是否应该被提交了，
        AutoSpeedActor.BatchMessage(Some(currentMsg),batchEndTimestamp,commit)
  }

  /**
    * 单条模式下，封装当前消息
    * @param currentMsg 当前消息
    * @return 封装后的消息
    */
  private def singleProcess(currentMsg:Any):Any = {
    batchEndTimestamp = getMessageTimestamp(currentMsg)  //每次处理消息时都把当前批次的结束时间设置为message产生的时间戳
    if(batchCounter == batchNumber){ //第一个批次挨个处理完了，
      batchCounter = 0 //批量处理的counter置为0也印证了第一个批次只是测试的，挨个处理的消息，
      log.debug(s"Reach an batch which from $batchStartTimestamp to $batchEndTimestamp ,time diff is ${batchEndTimestamp - batchStartTimestamp}")
      // 在一个批量内，时间跨度大于设定的批量阈值，则表示接收的消息比较慢
      if (batchEndTimestamp - batchStartTimestamp > batchInterval.toMillis ){
        batchStartTimestamp = batchEndTimestamp  //下个批次的开始时间更新为这个批次的结束时间
      }else{
        // 在一个批量内，时间跨度小于设定的批量阈值，则表示接收的消息比较快，进入批量模式
        timerGuard ! AutoSpeedActor.EnterBatch(System.currentTimeMillis())
        messageIntercept = batchProcess
      }
    }
    currentMsg //直接返回了？也就是说第一个批次的消息根本没有包装，还是单个的Insert Command单个插入的方式，
  }

  //拦截消息，处理消息的逻辑放在子类，在子类逻辑处理之前，这里会先拦截
  override def aroundReceive(receive: Receive, msg: Any): Unit = {
    val interceptedMessage = if(isAutoDriveMessage(msg) || isIntervalMessage(msg)){ //如果是自动驾驶的消息(即是Insert种类的command)，
      batchCounter += 1    //当前批次处理的数量，
      messageIntercept(msg)  //对消息进行包装，如果可能的话，返回一个 BatchMessage
    }else{
      msg
    }

    super.aroundReceive(receive, interceptedMessage)
  }
}

private[actor] class AutoSpeedActorGuard(timeout:Duration) extends Actor with ActorLogging{
  private var batchMode = false
  private implicit val executionContextExecutor: ExecutionContextExecutor = context.dispatcher

  override def receive: Receive = {
    case AutoSpeedActor.EnterBatch(systemTimestamp) =>
      log.debug(s"Enter batch mode at $systemTimestamp")
      batchMode = true
      context.system.scheduler.scheduleOnce(FiniteDuration(timeout._1,timeout._2),self,AutoSpeedActor.BatchInterval(systemTimestamp)) //发送一个BatchInterval的消息给自己，

    case AutoSpeedActor.LeaveBatch(systemTimestamp) =>
      log.debug(s"Leave batch mode at $systemTimestamp")
      batchMode = false

    case evt: AutoSpeedActor.BatchInterval =>
      log.debug(s"Receive an AutoSpeedActor.BatchInterval message $evt ,batchMode = $batchMode")
      if(batchMode){ //当前是批量模式，才给父监管者发 BatchInterval 消息，代表可能的超时
        context.system.scheduler.scheduleOnce(FiniteDuration(timeout._1,timeout._2),self,AutoSpeedActor.BatchInterval(System.currentTimeMillis()))
        context.parent ! evt  //给它的父亲发送一个 BatchInterval 的消息
      }
  }
}