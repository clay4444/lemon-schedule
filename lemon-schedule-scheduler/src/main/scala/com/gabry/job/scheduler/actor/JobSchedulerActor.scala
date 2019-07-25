package com.gabry.job.scheduler.actor

import java.sql.SQLIntegrityConstraintViolationException

import akka.actor.{ActorRef, Props, Status}
import com.gabry.job.core.actor.SimpleActor
import com.gabry.job.core.command.JobSchedulerCommand
import com.gabry.job.core.domain.Job
import com.gabry.job.core.po.{JobPo, SchedulePo}
import com.gabry.job.core.tools.UIDGenerator
import com.gabry.job.db.DataTables
import com.gabry.job.db.proxy.DataAccessProxyException
import com.gabry.job.db.proxy.command.DatabaseCommand
import com.gabry.job.db.proxy.event.DatabaseEvent
import com.gabry.job.quartz.{MessageRequireFireTime, MessageWithFireTime, QuartzSchedulerExtension}
import com.gabry.job.utils.{CronGenerator, Utils}

/**
  * Created by gabry on 2018/4/3 11:05
  */
object JobSchedulerActor{
  def props(dataAccessProxy: ActorRef,nodeAnchor:String):Props = Props(new JobSchedulerActor(dataAccessProxy,nodeAnchor))
}
/**
  * 作业调度器。根据提交的Job信息，创建对应的执行计划表
  * @param dataAccessProxy 数据操作代理actor
  * @param nodeAnchor JobSchedulerNode节点信息
  */
class JobSchedulerActor private (dataAccessProxy: ActorRef,nodeAnchor:String)  extends SimpleActor{
  private lazy val scheduler = QuartzSchedulerExtension(context.system)

  /**
    * 调度器调度的周期，配置文件中单位是分钟
    * 调度器在每个周期执行的时候，提前生成下个周期的作业
    */
  private val frequencyInSec = config.getDuration("scheduler.frequency").getSeconds

  //根据 jobtracker 发来的Job的信息创建 数据库Job实体
  private def createJobPo(job:Job,scheduleTime:Long):JobPo =

    JobPo(job.uid,job.name,job.className,job.getMetaJsonString(),job.dataTimeOffset,job.dataTimeOffsetUnit
      ,job.startTime,job.cron,job.priority,job.parallel,job.retryTimes
      ,Some(job.workerNodes.mkString(",")),job.cluster,job.group,job.timeOut,job.replaceIfExist
      ,None ,schedulerNode = Some(nodeAnchor)       //设置在当前的scheduler节点中调度
      ,scheduleFrequency = Some(frequencyInSec)     //调度器的调度周期是多少，提前配置好的，多长时间调度一次
      ,lastScheduleTime = Some(scheduleTime) )      //第一次调度的时间(传过来的时候取的当前时间)

  override def preStart(): Unit = {
    super.preStart()
    //这个actor启动的时候直接开始调度(执行)生成执行计划表的scheduler， 执行时返回的消息返回给自己，返回的消息类型是 调度频率的命令？那这个scheduler执行计划干啥了呢？就发个消息就完了？
    scheduler.schedule("JobScheduler",self,MessageRequireFireTime(JobSchedulerCommand.ScheduleJobFreq))  // 0 返回的就是cmd @ MessageWithFireTime(_,scheduledFireTime) =>
    log.info(s"JobSchedulerActor started at $selfAddress")
  }

  override def postStop(): Unit = {
    super.postStop()
    scheduler.shutdown() //停止scheduler
  }

  //调度的结束时间就是 开始时间往后延迟一个周期，为什么调度结束时间这样设置？是因为下个调度周期要开始调度了吗？因为下个周期会生成下个周期内的执行计划
  private def getScheduleStopTime(startTime:Long):Long =
    startTime + frequencyInSec * 1000 * 1

  /**
    * 调度作业(这算是比较核心的了)
    * 这个while循环内部，会把lastTriggerTime到lastTriggerTime往后延迟一个调度周期内，这段时间应该触发的执行计划，全部保存到数据库
    * @param scheduleTime     作业的开始执行时间 (也就是被调度的时间)
    * @param jobPo            作业实体
    * @param originCommand    command
    */
  private def scheduleJob(scheduleTime:Long,jobPo: JobPo,originCommand:AnyRef):Unit = {
    var lastTriggerTime = scheduleTime                //上次触发的时间设置为job的开始时间
    val stopTime = getScheduleStopTime(scheduleTime)  //结束时间 = 开始时间 + 一个调度周期
    while( lastTriggerTime < stopTime){
      //计算一个这个job在 lastTriggerTime 时间之后的下一次执行时间
      CronGenerator.getNextTriggerTime(jobPo.cron,lastTriggerTime) match {
        case Some(nextTriggerTime) =>
          log.debug(s"generate nextTriggerTime $nextTriggerTime for ${jobPo.name}")
          lastTriggerTime = nextTriggerTime        //上次触发的时间设置为：下一次应该被触发的时间，用这种方式依次找出在这个调度周期内应该执行的所有执行计划

          /**
            * 是否被调度这个字段一开始设置为false，nextTriggerTime下次触发时间，succeed是否成功
            * calcPostOffsetTime: 从下次触发的时间开始，往后偏移(根据偏移量和偏移量单位)，返回偏移后的时间，这是干嘛用的呢？
            */
          val schedulePo = SchedulePo(UIDGenerator.globalUIDGenerator.nextUID(),jobPo.uid,jobPo.priority,jobPo.retryTimes
            ,dispatched = false,nextTriggerTime,nodeAnchor,scheduleTime,succeed = false,
            Utils.calcPostOffsetTime(nextTriggerTime,jobPo.dataTimeOffset,jobPo.dataTimeOffsetUnit),null)

          dataAccessProxy ! DatabaseCommand.Insert(schedulePo,self,originCommand)

        case None =>
          log.info(s"can not generate next trigger time [${jobPo.cron}]")
          lastTriggerTime = stopTime
      }
    }
    // 更新最后一次生成的作业触发时间
    val newJobPo = jobPo.copy(lastGenerateTriggerTime = Some(lastTriggerTime)
      ,lastScheduleTime = Some(scheduleTime),schedulerNode = Some(nodeAnchor))

    dataAccessProxy ! DatabaseCommand.Update(jobPo,newJobPo,self,originCommand)
  }


  override def userDefineEventReceive: Receive = {
    case DatabaseEvent.Inserted(Some(schedulePo:SchedulePo),originEvent)=>
      log.debug(s"Schedule [${schedulePo.uid}] inserted to db for $originEvent")
    case Status.Failure(DataAccessProxyException(DatabaseCommand.Insert(_:SchedulePo,_,_),exception)) =>
      exception match {
        case _:SQLIntegrityConstraintViolationException =>
        // 由于会重新生成作业实例，此处的异常是正常的
        case unacceptableException:Throwable =>
          log.error(s"unacceptableException:$unacceptableException")
          log.error(unacceptableException,unacceptableException.getMessage)
      }
    case DatabaseEvent.Updated(oldRow:JobPo,row:Option[JobPo],originCommand) =>
      log.debug(s"JobPo $oldRow update to $row for $originCommand")
    case Status.Failure(DataAccessProxyException(DatabaseCommand.Update(oldRow:JobPo,row:JobPo,_,originCommand),exception)) =>
      log.error(exception,exception.getMessage)
      log.error(s"JobPo $oldRow update to $row error ,reason: $exception")

    /**
      *  1  ** quartz调度 JobScheduler 这个Job后，回复的消息 (每分钟收到一次)
      *  这个scheduledFireTime就是 JobScheduler 这个job每次执行，都会给当前actor发送一个scheduledFireTime，代表这个job的调度时间
      */
    case cmd @ MessageWithFireTime(_,scheduledFireTime) =>
      // 首先根据scheduledFireTime计算当前需要调度的起始时间和终止时间。
      // 从jobs获取最后一次调度的时间，计算未执行的作业的个数，和最后一个需要执行的时间，如果个数小于aheadNum或者待执行作业执行时间跨度小于调度周期，则补满
      // 2018年4月8日09:30:22：上面的方案太复杂，简化一下，先提前一个周期调度。如果某个作业最后一次调度的时间与调度的任务最后一次执行时间相差一个周期，则开始调度
      val scheduleTime = scheduledFireTime.getTime
      log.warning(s"scheduleTime=$scheduleTime")

      //去数据库查需要调度的作业
      dataAccessProxy ! DatabaseCommand.Select((DataTables.JOB,nodeAnchor,scheduleTime,frequencyInSec),self,cmd)

    case DatabaseEvent.Selected(Some(jobPo:JobPo),originCommand @ MessageWithFireTime(_,scheduledFireTime)) =>
      scheduleJob(scheduledFireTime.getTime,jobPo,originCommand)


    /**
      * 收到manager的调度信息（jobTracker发出的，然后由JobTrackerNode转发过来的），需要与周期性的调度区分开
      */
    case originCmd @ JobSchedulerCommand.ScheduleJob(job,replyTo) =>
      // 登记该作业的调度器信息字段到数据库，然后发送消息，给当前节点生成执行计划表
      log.info(s"开始调度作业 $job")
      // 首先删除已经生成的计划表中未执行的信息，然后重新生成
      val scheduleTime = System.currentTimeMillis()

      // 删除成功后开始生成作业执行计划表
      val jobPo = createJobPo(job,scheduleTime)
      // 更新作业信息
      scheduleJob(jobPo.startTime,jobPo,originCmd)

    case JobSchedulerCommand.StopScheduleJob(job) =>
      log.warning(s"收到停止调度job的消息$job")
    // TODO: 2018年4月12日17:52:59 停止调度job。删掉任务计划表、停止当前作业、发通知给JobTracker
    // TODO: 2018年4月12日17:54:31 Job类中需要增加是否强制停止当前作业的标志，如果不强制停止，会一直等作业执行完毕，那么等待多久呢？

  }
}
