package com.gabry.job.db.proxy.actor
import akka.actor.{Props, Status}
import akka.pattern.pipe
import com.gabry.job.core.Message
import com.gabry.job.core.actor.AutoSpeedActor
import com.gabry.job.core.po.JobPo
import com.gabry.job.db.DataTables
import com.gabry.job.db.access.JobAccess
import com.gabry.job.db.proxy.DataAccessProxyException
import com.gabry.job.db.proxy.command.DatabaseCommand
import com.gabry.job.db.proxy.event.DatabaseEvent
import com.gabry.job.utils.ExternalClassHelper._
import com.typesafe.config.Config

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
/**
  * Created by gabry on 2018/5/14 11:03
  */
object JobAccessProxy{
  def props(jobAccess:JobAccess,config:Config):Props = {
    val batchNumber = config.getIntOr("db.batch.number",1000)
    val batchInterval = config.getDurationOr("db.batch.interval",java.time.Duration.ofSeconds(3))
    Props(new JobAccessProxy(jobAccess,batchNumber,batchInterval,System.currentTimeMillis()))
  }
}
class JobAccessProxy private (jobAccess:JobAccess,batchNumber:Long,batchInterval:Duration,startTime:Long) extends AutoSpeedActor(batchNumber,batchInterval,startTime){
  private var cmdBuffer = List.empty[DatabaseCommand.Insert[JobPo]]  //插入命令缓存
  private val batchEnable = config.getBooleanOr("db.batch.enable",default = false)  //默认批量模式为false

  /**
    * 用户自定义事件处理函数
    */
  override def userDefineEventReceive: Receive = {

    //commit为false，非提交的命令，不处理，只添加缓存，
    case AutoSpeedActor.BatchMessage(Some(sourceMessage:DatabaseCommand.Insert[JobPo]),_ ,commit) if !commit =>
      cmdBuffer = sourceMessage :: cmdBuffer

    //commit为true，需要提交到数据库了
    case AutoSpeedActor.BatchMessage(sourceMessage: Option[DatabaseCommand.Insert[JobPo]],_ ,commit) if commit =>
      sourceMessage.foreach( msg => cmdBuffer = msg :: cmdBuffer )  //先添加到缓存
      val currentCmdBuffer = cmdBuffer
      log.info(s"Batch insert commit current batch size: ${currentCmdBuffer.length}")
      jobAccess.batchInsert(currentCmdBuffer.map(_.row).toArray).onComplete{      //这里直接调用jobAccess的批量插入的方法
        case Success(_) =>
          currentCmdBuffer.foreach{ insert =>
            insert.replyTo ! DatabaseEvent.Inserted(Some(insert.row),insert.originEvent)  //告诉消息的发送者，插入成功了，返回插入的数据，和 引起数据库操作的源命令行消息
          }
        case Failure(reason) =>
          currentCmdBuffer.foreach{ insert =>
            insert.replyTo ! Status.Failure(DataAccessProxyException(insert,reason)) //告诉发送方，插入失败，返回插入的数据和失败的原因，
          }
      }
      cmdBuffer = List.empty[DatabaseCommand.Insert[JobPo]]  //清空缓存

    //处理单个插入消息，并把处理结果告诉消息发送方
    case cmd @ DatabaseCommand.Insert(row:JobPo,replyTo,_) =>
      val insertJob = jobAccess.insert(row)
      insertJob.mapAll(
        insertedJobPo => DatabaseEvent.Inserted(Some(insertedJobPo),cmd.originEvent)  //注意返回的是事件类型，不是command 类型
        ,exception => DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender())

    //处理查询消息
    case selectByNameCmd @ DatabaseCommand.Select(row:JobPo,replyTo,originCommand)=>
      val select = jobAccess.selectOne(row.name)
      select.mapAll(
        selectedJobPo => DatabaseEvent.Selected(selectedJobPo,originCommand)
        ,exception => DataAccessProxyException(selectByNameCmd,exception))
        .pipeTo(replyTo)(sender())

    //处理更新消息
    case cmd @ DatabaseCommand.Update(oldRow:JobPo,row:JobPo,replyTo,originCommand) =>
      val update = jobAccess.update(row)
      update.mapAll(
        updatedNum => DatabaseEvent.Updated(oldRow,if(updatedNum>0) Some(row) else None,originCommand)
        ,exception => DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender())

    //选择在一个周期内需要调度的作业，nodeAnchor:调度器节点，scheduleTime:调度周期的时间，frequencyInSec:调度器周期
    case DatabaseCommand.Select((DataTables.JOB,nodeAnchor:String,scheduleTime:Long,frequencyInSec:Long),replyTo,originCommand) =>
      jobAccess.selectScheduleJob(nodeAnchor,scheduleTime,frequencyInSec){ jobPo =>
        replyTo ! DatabaseEvent.Selected(Some(jobPo),originCommand)
      }

    //寻找对应调度器负责调度的job
    case DatabaseCommand.Select((DataTables.JOB,nodeAnchor:String),replyTo,originCommand) =>
      jobAccess.selectJobsByScheduleNode(nodeAnchor){ job =>
        replyTo ! DatabaseEvent.Selected(Some(job),originCommand)
      }
  }

  /**
    * 获取当前消息的时间戳
    * @param msg 当前消息
    * @return 当前消息的时间戳
    */
  override def getMessageTimestamp(msg: Any): Long = msg match {
    case message:Message => message.at
    case _ => System.currentTimeMillis()
  }

  /**
    * 判断当前消息是否自动驾驶，
    *
    * @param msg 当前消息
    * @return true则对此类型的消息自动调整速率
    */
  override def isAutoDriveMessage(msg: Any): Boolean = msg match {
    case _:DatabaseCommand.Insert[JobPo] => batchEnable
    case _ => false
  }
}