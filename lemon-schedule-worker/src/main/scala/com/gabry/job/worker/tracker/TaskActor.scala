package com.gabry.job.worker.tracker

import java.util.concurrent.{Callable, FutureTask}

import akka.actor.{Props, ReceiveTimeout}
import com.gabry.job.core.actor.SimpleActor
import com.gabry.job.core.command.{TaskActorCommand, TaskCommand}
import com.gabry.job.core.domain._
import com.gabry.job.core.event.{TaskActorEvent, TaskEvent, TaskRunnerEvent}
import com.gabry.job.core.task.Task
import com.gabry.job.utils.Utils

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Created by gabry on 2018/3/23 17:32
  */
/**
  * 用actor封装task类,用来接收作业调度信息，控制并发数量
  * 一个TaskActor与一个Task具体实现一一对应；TaskActor调用Task对应的接口，进行初始化等操作，但是不负责具体的执行
  * @param taskActorInfo TaskActor参数信息
  *
  *
  * TaskTrackerActor 和 TaskActor 都是worker初始化的时候自己启动的，TaskTrackerActor对应jar包，TaskActor对应class，
  * worker这个角色启动的时候所有Task class都初始化好了，Task实例都已经初始化完了，
  *
  * 每次任务调度的消息发到这里的时候，会启动一个新的Actor(TaskRunner)，运行，
  *
  */
class TaskActor(taskActorInfo:TaskActorInfo) extends SimpleActor{
  /**
    * 当前执行的Task数量，也就是并发数
    */
  private var executingTaskNum = 0
  /**
    * Task实例，通过反射获取
    */
  private val task = taskActorInfo.claz.newInstance().asInstanceOf[Task]

  /**
    * 启动之前调用task.initialize()
    */
  override def preStart(): Unit = {
    super.preStart()
    task.initialize()
  }

  /**
    * 关闭之后调用task.destroy()
    */
  override def postStop(): Unit = {
    super.postStop()
    task.destroy()
  }

  /**
    * 处理收到的消息
    */
  override def userDefineEventReceive: Receive = {

    /**
      * 0、  这个RunTask命令应该是scheduler节点发过来的，也就是一个调度命令
      * 这个命令是由scheduler节点发出的，先发给TaskTracker，然后转发给TaskActor
      */
    case runCmd @ TaskActorCommand.RunTask( jobContext,replyTo ) =>     //注意这里的replyTo指的是聚合任务状态的 actor
      log.info(s"收到了runCmd命令 $runCmd")
      // 收到调度命令后，判断当前并发度是否满足条件，满足后创建TaskRunner执行具体的业务逻辑
      if( executingTaskNum <= taskActorInfo.classInfo.parallel ){

        executingTaskNum += 1

        //构建TaskRunnnerActor 需要的info
        val taskRunnerInfo = TaskRunnerInfo(taskActorInfo.cluster,taskActorInfo.group,task,taskActorInfo.classInfo)

        //创建 TaskRunnerActor，每次调度都会生成新的 TaskRunnerActor，actor name是 (jobName-triggerTime)
        val taskRunner = context.actorOf(Props.create(classOf[TaskRunnerActor],taskRunnerInfo),jobContext.job.name+"-"+jobContext.schedule.triggerTime)
        context.watchWith(taskRunner,TaskRunnerEvent.Stopped(taskRunner))  //death watch
        replyTo ! TaskEvent.Started(taskRunner,jobContext)   //告诉scheduler，TaskRunner已经启动了；

        taskRunner ! runCmd  //告诉TaskRunner，开始执行，

      }else{
        // 如果超过了并发，则告诉发送方需要重新调度，发送方应该是schedulerNode
        // 重新调度时，并不一定还是当前节点
        // TODO: 2018年4月11日13:41:39 此处去掉并发的限制，
        replyTo ! TaskActorEvent.Overloaded(jobContext)
      }

    //death watch收到的消息，TaskRunner停止运行
    case evt @ TaskRunnerEvent.Stopped(taskRunner) =>
      val stopAt = System.currentTimeMillis()
      executingTaskNum -= 1
      log.warning(s"task runner $taskRunner stopped,alive time ${Utils.formatAliveTime(evt.at,stopAt)}")
  }
}

/**
  * 用actor封装task执行，控制重试、超时、取消等机制
  * @param taskRunnerInfo taskRunner信息
  */
class TaskRunnerActor(taskRunnerInfo:TaskRunnerInfo) extends SimpleActor{
  // 需要设置子actor的监督机制，确保失败后不会自动重启
  /**
    * 当前作业的执行命令
    */
  private var runCmd:TaskActorCommand.RunTask = _
  /**
    * 可中断的Task
    */
  private var interruptableTask:InterruptableTask = _
  private val checkDependencySleepTime = config.getDuration("worker.check-dependency-sleep-time").toMillis   //检查依赖是否完成的时间间隔
  private var dependencyPassed = false  //默认依赖检查不通过
  private var sendWaiting = false      //这个玩意的作用就是
  // 如果没有达到指定次数则进行重试
  /**
    * 是否可以重新运行
    * @return 如果重试次数小于最大重试次数则可以重新运行
    */
  private def canReRun():Boolean = runCmd.jobContext.retryId < runCmd.jobContext.job.retryTimes

  override def userDefineEventReceive: Receive = {

    //3、 任务聚合actor返回的依赖状态
    case evt @ TaskEvent.DependencyState(passed) =>
      dependencyPassed = passed
      //当前时间 - 这个job的触发时间 > 这个Job设置的超时时间；就不再继续运行了，直接修改状态为Timeout
      if( evt.at - runCmd.jobContext.schedule.triggerTime > taskRunnerInfo.classInfo.defaultTimeOut.min(runCmd.jobContext.job.timeOut)*1000 ){
        self ! ReceiveTimeout
      }else{
        self ! runCmd  //给自己发送RunTask消息，继续运行；
      }
    /**
      * 1、   收到运行作业的命令
      * 首先检查作业的依赖是否成功，需要考虑检查作业的时限和次数，以及每次检查的时间间隔
      * 依赖检查通过后，就可以实际的运行作业了
      */
    case cmd:TaskActorCommand.RunTask =>
      runCmd = cmd

      if(dependencyPassed){ //依赖检查通过后，开始执行
        interruptableTask = new InterruptableTask(taskRunnerInfo.task,runCmd.jobContext,taskRunnerInfo.classInfo.parallel)
        // 超时时间，取最小值，单位是秒
        val timeout = taskRunnerInfo.classInfo.defaultTimeOut.min(runCmd.jobContext.job.timeOut)

        // 设定当前作业的超时时间，超时后中断Task，重新调用
        context.setReceiveTimeout(timeout seconds)

        // 放到future执行，也就是放到另外一个单独线程异步执行
        // 执行结束需要发消息给self
        Future{
          runCmd.replyTo ! TaskEvent.Executing(runCmd.jobContext) //修改任务为执行中
          interruptableTask.run()
        }.onComplete{
          case Success(runResult) =>  //执行完成
            log.debug(s"interruptableTask执行结果$runResult")
            interruptableTask.get() match {
              case Success(taskReturn) =>   //完成且成功
                log.debug(s"作业执行成功 result = $taskReturn,get = ${interruptableTask.get()}")
                self ! TaskEvent.Succeed(runCmd.jobContext)
              case Failure(taskException)=>   //完成但失败
                log.warning(s"作业执行出现异常 $taskException")
                self ! TaskEvent.Failed(taskException.getMessage,runCmd.jobContext)
            }
          case Failure(runException) =>  //直接失败
            log.warning(s"作业执行失败:${runException.getMessage}")
            self ! TaskEvent.Failed(runException.getMessage,runCmd.jobContext)
        }
      }else{
        if(!sendWaiting){  //这个sendWaiting存在的意义就是在任务刚开始运行的时候设置为 waiting ？
          runCmd.replyTo ! TaskEvent.Waiting(runCmd.jobContext)
          sendWaiting = true
        }
        // 此处休眠3秒，在发送运行作业的命令，防止死循环
        // 因为RunTask和DependencyState命令会循环触发
        Thread.sleep(checkDependencySleepTime)

        //2、  休眠之后继续查询依赖任务状态
        runCmd.replyTo ! TaskCommand.CheckDependency(runCmd.jobContext.job.uid,runCmd.jobContext.dataTime,self)  //runCmd.replyTo 指的是聚合任务状态的 actor
      }


    /**
      * 提前终止作业,直接退出，不再重试
      * 用户主动取消掉
      */
    // TODO: 2018年4月11日16:16:16 暂时取消该功能。具体由谁进行取消还没有定
//    case TaskActorCommand.CancelTask(replyTo) =>
//      context.setReceiveTimeout(Duration.Undefined)
//      val interrupt = interruptableTask.interrupt()
//      log.warning(s" ${runCmd.jobContext} 作业被终止:$interrupt")
//      val cancelEvent = TaskEvent.Cancelled(replyTo,runCmd.jobContext)
//      replyTo ! cancelEvent
//      runCmd.replyTo ! cancelEvent
//      context.stop(self)

    /**
      * 作业执行超时，杀掉作业并重新运行，如果重试失败则退出
      */
    case timeout:ReceiveTimeout =>
      val interrupt = interruptableTask.interrupt()  //中断作业
      log.info(s"作业执行超时 $timeout，中断作业 $interrupt")
      runCmd.replyTo ! TaskEvent.Timeout(runCmd.jobContext)  //修改数据库Task状态为Timeout（但未停止）
      context.setReceiveTimeout(Duration.Undefined)
      if(canReRun()){
        self ! runCmd.copy(jobContext = runCmd.jobContext.copy(retryId = runCmd.jobContext.retryId + 1 ))  //发送自己一个RunTask消息，然后继续运行；
      }else{
        // 如果reRun失败则退出此次调用
        runCmd.replyTo ! TaskEvent.TimeOutStopped(runCmd.jobContext) //超时停止；
        context.stop(self)  //停止Actor
      }

    /**
      * 作业执行成功
      */
    case evt @ TaskEvent.Succeed(jobContext) =>
      // 作业执行成功后，将成功消息发送给汇报者
      // 注意，此处不能是原来的jobContext，因为task执行时可能会修改jobContext
      runCmd.replyTo ! evt  //修改mysql任务状态为成功
      log.info(s"$jobContext 执行成功，现在退出")
      context.stop(self)

    /**
      * 作业执行失败
      */
    case evt :TaskEvent.Failed =>
      log.info(s"$evt 执行失败")
      context.setReceiveTimeout(Duration.Undefined)
      runCmd.replyTo ! evt    //修改mysql任务状态为失败
      if(canReRun()){
        self ! runCmd.copy(jobContext = runCmd.jobContext.copy(retryId = runCmd.jobContext.retryId + 1 ))  //继续执行；
      }else{
        // 如果reRun失败则退出此次调用
        runCmd.replyTo ! TaskEvent.MaxRetryReached(runCmd.jobContext)  //已到达最大重试次数，停止任务；
        context.stop(self)
      }
  }
}

/**
  * 用Callable封装task的具体执行，这样就可以
  * @param task 封装的task
  * @param jobContext 作业执行上下文
  * @param parallel 作业并发度
  */
class InterruptableTask(task:Task, jobContext: JobContext, parallel:Int){

  /**
    * 封装Task，使其可中断
    * @param task 封装的task
    * @param jobContext 作业执行上下文
    * @param taskIndex 作业需要
    * @param parallel 作业并发度
    */
  private[this] class InnerTask(task:Task, jobContext: JobContext, taskIndex:Int, parallel:Int) extends Callable[Long]{
    override def call(): Long = {
      task.run(jobContext,taskIndex,parallel)
      Thread.currentThread().getId
    }
  }

  /**
    * FutureTask封装Task使其可以中断
    * 这里 taskIndex( 0 ~ paraller-1 ) 竟然取的 retryId？
    */
  private val futureTask = new FutureTask[Long](new InnerTask(task,jobContext,jobContext.retryId,parallel))


  /**
    * 运行Task
    */
  def run():Unit = futureTask.run()

  /**
    * 中断Task
    */
  def interrupt():Boolean = futureTask.cancel(true)

  /**
    * 获取执行结果
    * @return 执行结果的Try封装
    */
  def get():Try[Long] = Try(futureTask.get())
}