package com.gabry.job.db.proxy.actor
import akka.pattern.pipe
import com.gabry.job.core.actor.SimpleActor
import com.gabry.job.core.domain.UID
import com.gabry.job.core.po.SchedulePo
import com.gabry.job.db.DataTables
import com.gabry.job.db.access.ScheduleAccess
import com.gabry.job.db.proxy.DataAccessProxyException
import com.gabry.job.db.proxy.command.DatabaseCommand
import com.gabry.job.db.proxy.event.DatabaseEvent
import com.gabry.job.utils.ExternalClassHelper._
/**
  * Created by gabry on 2018/5/14 11:12
  */
class ScheduleAccessProxy(scheduleAccess:ScheduleAccess) extends SimpleActor{
  /**
    * 用户自定义事件处理函数
    */
  override def userDefineEventReceive: Receive = {

    //插入消息
    case cmd @ DatabaseCommand.Insert(schedulePo:SchedulePo,replyTo,originEvent) =>
      scheduleAccess.insert(schedulePo).mapAll(
        insertedSchedulePo => DatabaseEvent.Inserted(Some(insertedSchedulePo),originEvent),
        exception => DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender()) //返回消息给这个command的发送方

    //设置已调度
    case cmd @ DatabaseCommand.UpdateField(DataTables.SCHEDULE,scheduleUid:UID,fields @ Array("DISPATCH"),replyTo,originEvent) =>
      scheduleAccess.setDispatched(scheduleUid,dispatched = true).mapAll(
        updateNum =>DatabaseEvent.FieldUpdated(DataTables.SCHEDULE,fields,updateNum,Some(scheduleUid),originEvent), //跟新schedule表，更新的字段是DISPATCH，更新后影响的行数是updateNum，oid，引起数据库操作的源命令行消息
        exception =>DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender())

    //调度计划设置为成功
    case cmd @ DatabaseCommand.UpdateField(DataTables.SCHEDULE,scheduleUid,fields @ Array("SUCCEED"),replyTo,originEvent) =>
      scheduleAccess.setSucceed(scheduleUid,succeed = true).mapAll(
        updateNum => DatabaseEvent.FieldUpdated(DataTables.SCHEDULE,fields,updateNum,Some(scheduleUid),originEvent),
        exception => DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender())

    //调度计划设置为失败
    case cmd @ DatabaseCommand.UpdateField(DataTables.SCHEDULE,scheduleUid,fields @ Array("SUCCEED","FALSE"),replyTo,originEvent) =>
      scheduleAccess.setSucceed(scheduleUid,succeed = false).mapAll(
        updateNum => DatabaseEvent.FieldUpdated(DataTables.SCHEDULE,fields,updateNum,Some(scheduleUid),originEvent),
        exception =>DataAccessProxyException(cmd,exception))
        .pipeTo(replyTo)(sender())

    //根据scheduleNode、triggerTime选择待调度的任务，这里有问题吧？最后一个参数定义的是 maxNum：一次调用最多返回的记录数，这里定义的是并行度 ？ @bug
    case DatabaseCommand.Select((DataTables.SCHEDULE,jobUid:UID,nodeAnchor:String,triggerTime:Long,jobParallel:Int),replyTo,originCommand) =>
      scheduleAccess.selectUnDispatchSchedule(jobUid,nodeAnchor,triggerTime,jobParallel){ schedulePo =>
        replyTo ! DatabaseEvent.Selected(Some(schedulePo),originCommand)
      }
  }
}
