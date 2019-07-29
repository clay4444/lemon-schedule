package com.gabry.job.scheduler.actor

import akka.actor.{ActorRef, Props, Status}
import akka.routing.{ActorRefRoutee, RoundRobinRoutingLogic, Router}
import com.gabry.job.core.actor.SimpleActor
import com.gabry.job.core.command.{JobTaskDispatcherCommand, TaskActorCommand, TaskDispatcherCommand}
import com.gabry.job.core.domain.{JobContext, UID}
import com.gabry.job.core.event.TaskTrackerEvent
import com.gabry.job.core.po.{JobPo, SchedulePo}
import com.gabry.job.db.DataTables
import com.gabry.job.db.proxy.DataAccessProxyException
import com.gabry.job.db.proxy.command.DatabaseCommand
import com.gabry.job.db.proxy.event.DatabaseEvent
import com.gabry.job.quartz.{MessageRequireFireTime, MessageWithFireTime, QuartzSchedulerExtension}
import com.gabry.job.utils.Utils

/**
  * Created by gabry on 2018/4/3 11:06
  */
object JobTaskDispatcherActor{
  def props(dataAccessProxy: ActorRef,nodeAnchor:String,aggregatorActor:ActorRef):Props =
    Props(new JobTaskDispatcherActor(dataAccessProxy,nodeAnchor,aggregatorActor))
}

/**
  * 做Task(执行计划)分发，
  * @param dataAccessProxy  数据访问Actor
  * @param nodeAnchor       jobSchedulerNode 集群角色的节点值
  * @param aggregatorActor  任务状态聚合Actor
  */
class JobTaskDispatcherActor private (dataAccessProxy: ActorRef,nodeAnchor:String,aggregatorActor:ActorRef) extends SimpleActor{

  // TODO: 所以每次只从数据库查询出当前节点需要执行的任务，考虑到该数量仍然可能会很多，需要实现翻页的机制，一期先不做
  // 此处需要分开两个子actor
  // 一个actor提前1个周期将符合调度条件的数据从数据库查数据，将查询到的数据放入List
  // 另一个actor按照当前调度周期，从list中选择符合调度条件的任务发送出去
  // 后期需要考虑大面积的作业延迟的情况下，如何平滑的调度任务
  private lazy val scheduler = QuartzSchedulerExtension(context.system)

  //worker 路由？
  private var taskTrackerRouter = Router(RoundRobinRoutingLogic(),Vector.empty[ActorRefRoutee])

  override def preStart(): Unit = {
    super.preStart()
    scheduler.start()

    /**
      * 此处也可以用scheduleOnce来实现，但为scheduleOnce无法知道当前的调度周期，即无法度量调度的延迟时间。
      * 0、   实例化第二个job，开始收到 JobTaskDispatcher(由quartz调度) 每分钟一次的消息
      */
    scheduler.schedule("JobTaskDispatcher",self,MessageRequireFireTime(JobTaskDispatcherCommand.Dispatch))
  }
  override def postStop(): Unit = {
    super.postStop()
    scheduler.shutdown(true)
  }

  override def userDefineEventReceive: Receive = {

    /**
      * 5.1、 更新执行计划为已调度之后，开始给 worker 分发任务
      */
    case DatabaseEvent.FieldUpdated(DataTables.SCHEDULE,Array("DISPATCH"),_:Int,Some(_:UID),DatabaseEvent.Selected(Some(schedulePo:SchedulePo),TaskDispatcherCommand.DispatchJob(job,triggerTime)))=>
      log.debug(s"${schedulePo.uid} dispatched update to true")
      // 从worker中选择符合条件的actor发送执行命令
      // 选择条件有：节点即IP地址，分组，className
      // 此处需要计算当前任务的数据时间、

      //以triggerTime为基准，偏移Job指定的偏移量，做什么的？
      val jobContext = JobContext(job,schedulePo,Utils.calcPostOffsetTime(triggerTime,job.dataTimeOffset,job.dataTimeOffsetUnit))
      val runCommand = TaskActorCommand.RunTask(jobContext,aggregatorActor)  //jobContext, replyTo
      val routees = taskTrackerRouter.routees.filter{
        case ActorRefRoutee( actorRef ) =>
          val cluster = actorRef.path.address.system   //集群名称，
          val host = actorRef.path.address.host.getOrElse("localhost")  //host 域名
          val group = actorRef.path.name              //分组对应worker的 path name ?
          val ipMatch = job.workerNodes match {
            case Some(workers) if workers != "" =>
              workers.split(",").contains(host) //找指定host的机器
            case Some(_) => true
            case None => true
          }
          cluster == job.cluster && group == job.group && ipMatch //都匹配，说明可以往这个节点发任务，
        case _ =>
          true
      }
      taskTrackerRouter.withRoutees(routees).route(runCommand,self) //设置发送方为自己，

    /**
      * 4、根据job，triggerTime 找到的这个调度周期需要执行的执行计划 ，这个也是通过消息的形式一个一个返回的，
      * triggerTime 就是 scheduledFireTime
      */
    case evt @ DatabaseEvent.Selected(Some(schedulePo:SchedulePo),originCommand @ TaskDispatcherCommand.DispatchJob(job,triggerTime)) =>
      log.debug(s"Dispatching schedule $schedulePo for $job at $triggerTime")

      dataAccessProxy ! DatabaseCommand.UpdateField(DataTables.SCHEDULE,schedulePo.uid,Array("DISPATCH"),self,evt)

    /**
      * 5.2、  修改为已调度失败，
      */
    case Status.Failure(DataAccessProxyException(DatabaseCommand.UpdateField(DataTables.SCHEDULE,_:UID,Array("DISPATCH"),_,DatabaseEvent.Selected(Some(_:SchedulePo),TaskDispatcherCommand.DispatchJob(job,_))),exception))=>
      log.error(exception,exception.getMessage)
      log.error(s"Job Schedule failed,can not set dispatch flag for $job,reason: ${exception.getMessage}")


    /**
      * 3、job， triggerTime 表示这次JobTaskDispatcher(quartz消息，分发task用的)，触发的时间
      */
    case DatabaseEvent.Selected(_:Option[JobPo],originCommand @ TaskDispatcherCommand.DispatchJob(job,triggerTime)) =>
      // 然后选择可调度的task
      log.debug(s"Dispatching Job for $job at $triggerTime")
      dataAccessProxy ! DatabaseCommand.Select((DataTables.SCHEDULE,job.uid,nodeAnchor,triggerTime,job.parallel),self,originCommand)

    /**
      * 1、  定时收到分发消息，从db中获取符合调度时间的任务
      */
    case cmd @ MessageWithFireTime(_,scheduledFireTime) =>
      val triggerTime = scheduledFireTime.getTime
      log.info(s"TriggerEvent 当前调度时间 ${Utils.formatDate(triggerTime)},$triggerTime")
      // 先选择该节点负责的所有Job
      dataAccessProxy ! DatabaseCommand.Select((DataTables.JOB,nodeAnchor),self,cmd)
//      jobAccess.selectJobsByScheduleNode(nodeAnchor){ job =>
//        self ! DatabaseEvent.Selected(Some(job),TaskDispatcherCommand.DispatchJob(job,triggerTime))
//      }

    /**
      * 2、 找到这个scheduler负责的job(一个一个返回的)，然后给自己发送 DatabaseEvent.Selected 消息
      */
    case DatabaseEvent.Selected(Some(job:JobPo),MessageWithFireTime(_,scheduledFireTime)) =>
      log.debug(s"Dispatching Job for $job at ${scheduledFireTime.getTime}")
      self ! DatabaseEvent.Selected(Some(job),TaskDispatcherCommand.DispatchJob(job,scheduledFireTime.getTime))


    case evt @ TaskTrackerEvent.TaskTrackerStarted(taskTracker) =>
      val taskTrackerKey = taskTracker.path.elements.mkString(",")
      log.info(s"TaskTracker启动 $taskTracker $taskTrackerKey")
      taskTrackerRouter = taskTrackerRouter.addRoutee(taskTracker)

    case evt @ TaskTrackerEvent.TaskTrackerStopped(taskTracker) =>
      log.info(s"TaskTracker停止 $taskTracker")
      val taskTrackerKey = taskTracker.path.elements.mkString(",")
      taskTrackerRouter = taskTrackerRouter.removeRoutee(taskTracker)
    case unKnowMessage =>
      log.error(s"unKnowMessage $unKnowMessage")
  }
}
