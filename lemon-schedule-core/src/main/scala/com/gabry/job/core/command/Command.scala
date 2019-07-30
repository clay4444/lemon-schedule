package com.gabry.job.core.command

import akka.actor.ActorRef
import com.gabry.job.core.Message
import com.gabry.job.core.domain._
import com.gabry.job.core.po.JobPo

/**
  * Created by gabry on 2018/3/23 17:38
  * 命令的抽象
  */
trait Command extends Message

/**
  * 需要回复的命令
  */
trait Reply{
  /**
    * 每条命令都会有一个消息的返回者，即命令被完整处理后应该把消息返回给谁（不一定是命令的发送者）
    * @return 命令执行后汇报的actor
    */
  def replyTo:ActorRef
}

/**
  * 需要回复的命令
  * DatabaseCommand 就实现了这个接口
  */
trait ReplyCommand extends Command with Reply

object TaskWorkerCommand {
  final case class StartTaskTracker(taskTrackerInfo:TaskTrackerInfo, replyTo:ActorRef) extends ReplyCommand
}

object TaskTrackerCommand{
  final case class StartTaskActor(taskClassInfo:TaskClassInfo,claz:Class[_], replyTo:ActorRef) extends ReplyCommand
}

/**
  * TaskActor 命令消息
  */
object TaskActorCommand{
  //给TaskRunner发的，
  final case class RunTask(jobContext: JobContext,replyTo:ActorRef) extends ReplyCommand
  //final case class CancelTask(replyTo:ActorRef)extends ReplyCommand
}
object TaskCommand{
  final case class CheckDependency(jobUid:UID, dataTime:Long, replyTo:ActorRef) extends ReplyCommand
}

//JobTracker 命令消息
object JobTrackerCommand{
  //提交Job
  final case class SubmitJob(job:Job,dependency: Array[Dependency],replyTo:ActorRef) extends ReplyCommand
  //调度Job
  final case class ScheduleJob(job:Job,replyTo:ActorRef) extends ReplyCommand
}

//JobScheduler 命令消息
object JobSchedulerCommand{
  //调度频率
  final case class ScheduleJobFreq(scheduleTime:Long,replyTo:ActorRef) extends ReplyCommand
  //开始调度
  final case class ScheduleJob(job:Job,replyTo:ActorRef) extends ReplyCommand
  //停止调度
  final case class StopScheduleJob(job:Job) extends Command
}

object JobTaskDispatcherCommand {
  final case object Dispatch extends Command
}
object JobClientCommand{
  final case class SubmitJob(job:Job,dependency: Array[Dependency]) extends Command
  final case class CancelJob(jobId:Long,force:Boolean) extends Command
  final case object Start extends Command
  final case object Stop extends Command
}

/**
  * 任务调度的 命令消息
  */
trait TaskDispatcherCommand extends Command
object TaskDispatcherCommand{
  final case class DispatchJob(job:JobPo,triggerTime:Long) extends TaskDispatcherCommand
}