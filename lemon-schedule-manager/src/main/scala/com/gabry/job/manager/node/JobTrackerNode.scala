package com.gabry.job.manager.node

import akka.actor.{ActorRef, Props, RootActorPath}
import akka.cluster.Member
import akka.routing.{ActorSelectionRoutee, RoundRobinRoutingLogic, Router}
import com.gabry.job.core.command.{JobSchedulerCommand, JobTrackerCommand}
import com.gabry.job.core.constant.Constants
import com.gabry.job.core.domain.{Dependency, UID}
import com.gabry.job.core.event.JobTrackerEvent
import com.gabry.job.core.node.{ClusterNode, ClusterNodeProps}
import com.gabry.job.core.po.DependencyPo
import com.gabry.job.db.proxy.DataAccessProxy
import com.gabry.job.manager.actor.JobTrackerActor

import scala.concurrent.ExecutionContextExecutor
/**
  * Created by gabry on 2018/3/29 10:38
  * 2018年5月2日10:06:46：拆分JobTrackerNode部分功能，避免node节点的心跳信息无法发送给集群，导致集群误以为节点down掉
  */
object JobTrackerNode extends ClusterNodeProps{
  override def props(args: Any*): Props = Props(new JobTrackerNode)
  override def props: Props =  Props(new JobTrackerNode)
  def createDependencyPo(jobId:UID, dep: Dependency):DependencyPo =
    DependencyPo(dep.uid,jobId,dep.dependJobUid,dep.timeOffset,dep.timeOffsetUnit,dep.timeOffsetUnit.toMillis(dep.timeOffset))

  override val daemonName: String = "JobTracker"
}

/**
  * 接收客户端的信息，将Job插入队列，供Scheduler进行调度
  * 同时监控Scheduler的状态，随时调整Job所属的Scheduler
  * 尽量将Job分发给Scheduler离Task近的节点
  */
class JobTrackerNode extends ClusterNode{

  private var schedulerRouter = Router(RoundRobinRoutingLogic(),Vector.empty[ActorSelectionRoutee])
  private var jobTracker:ActorRef = _
  // private val dataAccessFactory = DatabaseFactory.getDataAccessFactory(config).get

  //准备一个专门用于访问数据库的线程池
  private implicit lazy val databaseIoExecutionContext: ExecutionContextExecutor = context.system.dispatchers.lookup("akka.actor.database-io-dispatcher")
  private var databaseAccessProxy:ActorRef = _     //访问数据库的一个代理，作为JobTrackerNode的孩子


  override def preStart(): Unit = {
    super.preStart()
    //dataAccessFactory.init()

    //创建代理actor，负责数据访问操作；
    databaseAccessProxy = context.actorOf(Props.create(classOf[DataAccessProxy],databaseIoExecutionContext),"DataAccessProxy")

    //又创建一个jobTracker Actor，作为子孩子，还把数据访问的actor传递给了这个 jobTracker Actor
    jobTracker = context.actorOf(JobTrackerActor.props(databaseAccessProxy))
    context.watch(jobTracker)
  }

  override def postStop(): Unit = {
    super.postStop()
    context.stop(jobTracker)
    //dataAccessFactory.destroy()
  }

  /**
    * 也就是说：JobTrackerNode只处理两种消息：
    * 1.提交Job，通过JobTrackerActor里的databaseAccessProxy这个actor来做(也就是使用其中的JobAccessProxy)
    * 2.调度Job，直接给调度节点发送了一条 JobSchedulerCommand.ScheduleJob 的消息，并设置发送方为自己，（调度job的消息是jobTracker发给它的）
    *
    * 所以流程是：JobTrackerNode 接收一个Job，然后把插入Job的工作交给JobTrackerActor，也就是jobTracker，jobTracker 通过 JobTrackerNode 传给它的DataAccessProxy往数据库插入Job信息(可能有存在就更新的情况)，
    * 插入依赖信息。然后 jobTracker 又反过来，把刚刚插入的这个job信息，传给JobTrackerNode(通过发消息的方式)，让JobTrackerNode进行调度( 因为只有JobTrackerNode可以路由到调度器 )
    */
  override def userDefineEventReceive: Receive = {
    /**
      * 收到客户端提交Job的命令 给jobTracker发消息，
      * 将Job插入数据库，并将插入的结果，以JobInserted事件的形式pipe给 jobTracker 自己
      */
    case originCmd @ JobTrackerCommand.SubmitJob(job,_,_) =>
      log.debug(s"Receive SubmitJob Command $originCmd")
      jobTracker ! originCmd

    //开始调度作业，发送消息给调度节点（这个消息是 jobTracker 这个子actor 发送给他的）
    case JobTrackerCommand.ScheduleJob(job,replyTo) =>
      if(schedulerRouter.routees.nonEmpty){
        schedulerRouter.route(JobSchedulerCommand.ScheduleJob(job,self),self)   //设置回复给自己？可是self没有定义处理这个的返回消息的逻辑啊？
        log.info(s"Send ScheduleJob command to scheduler job.id = ${job.uid}")
        // 此处将插入后更新的Job对象发送给reply
        replyTo ! JobTrackerEvent.JobSubmitted(job) //成功加入调度了，
      }else{
        replyTo ! JobTrackerEvent.JobSubmitFailed("No Scheduler node found")  //没有可用的调度节点
      }
  }

  /**
    * 当发现scheduler节点加入集群时，加到routee中，在父类的 MemberUp事件发生时 回调；
    * @param member 加入集群的节点
    */
  override def register(member: Member): Unit = {
    if(member.hasRole(Constants.ROLE_SCHEDULER_NAME)){
      val scheduleNode = context.system.actorSelection(RootActorPath(member.address)/ "user" / Constants.ROLE_SCHEDULER_NAME)
      schedulerRouter = schedulerRouter.addRoutee(scheduleNode)
    }
  }

  //从routee中取消这个Member，在父类的 UnreachableMember 事件发生时 回调；
  override def unRegister(member: Member): Unit = {
    if(member.hasRole(Constants.ROLE_SCHEDULER_NAME)){
      val scheduleNode = context.system.actorSelection(RootActorPath(member.address)/ "user" / Constants.ROLE_SCHEDULER_NAME)
      schedulerRouter = schedulerRouter.removeRoutee(scheduleNode)
    }
  }
}