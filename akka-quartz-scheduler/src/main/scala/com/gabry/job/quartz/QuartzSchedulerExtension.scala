package com.gabry.job.quartz

import java.text.ParseException
import java.util.{Date, TimeZone}

import akka.actor._
import akka.event.{EventStream, Logging}
import com.typesafe.config.ConfigFactory
import org.quartz._
import org.quartz.core.jmx.JobDataMapSupport
import org.quartz.impl.DirectSchedulerFactory
import org.quartz.simpl.{RAMJobStore, SimpleThreadPool}
import org.quartz.spi.JobStore

import scala.collection.{immutable, mutable}
import scala.util.control.Exception._

/**
  * 为下面的扩展指定一个ExtensionId，用来获取它的实例
  * extends ExtensionId[CountExtensionImpl] with ExtensionIdProvider
  */
object QuartzSchedulerExtension extends ExtensionKey[QuartzSchedulerExtension] {
  //这个 ExtensionKey里帮我们实现了lookup方法(返回this 配置我们的扩展在当前的ActorSystem启动的时候被加载)和createExtension方法(被akka调用的，实例化我们的扩展)

  //为指定的ActorSystem返回我们定义的扩展，
  override def get(system: ActorSystem): QuartzSchedulerExtension = super.get(system)
}

/**
 * Note that this extension will only be instantiated *once* *per actor system*.
 * 定义一个具有某些功能的扩展
 */
class QuartzSchedulerExtension(system: ExtendedActorSystem) extends Extension {

  private val log = Logging(system, this)


  // todo - use of the circuit breaker to encapsulate quartz failures?
  // 考虑使用熔断机制实现quartz的故障？
  def schedulerName = "QuartzScheduler~%s".format(system.name)

  //a withFallback b  :a和b合并，如果有相同的key，以a为准
  protected val config = system.settings.config.withFallback(defaultConfig).getConfig("akka.quartz").root.toConfig

  // 设置必要参数，如果用户没有设置，就取下面的值
  // For config values that can be omitted by user, to setup a fallback
  lazy val defaultConfig =  ConfigFactory.parseString("""
    akka.quartz {
      threadPool {
        threadCount = 1
        threadPriority = 5
        daemonThreads = true
      }
      defaultTimezone = UTC
    }
                                                      """.stripMargin)  // todo - boundary checks

  // The # of threads in the pool
  val threadCount = config.getInt("threadPool.threadCount")

  //优先级应该在1和10之间
  // Priority of threads created. Defaults at 5, can be between 1 (lowest) and 10 (highest)
  val threadPriority = config.getInt("threadPool.threadPriority")
  require(threadPriority >= 1 && threadPriority <= 10,
    "Quartz Thread Priority (akka.quartz.threadPool.threadPriority) must be a positive integer between 1 (lowest) and 10 (highest).")

  // 如果指定使用非守护线程，可能会使 akka / jvm 停止异常
  // Should the threads we create be daemonic? FYI Non-daemonic threads could make akka / jvm shutdown difficult
  val daemonThreads_? = config.getBoolean("threadPool.daemonThreads")

  // 除非另有说明，否则使用默认时区
  // Timezone to use unless specified otherwise
  val defaultTimezone = TimeZone.getTimeZone(config.getString("defaultTimezone"))

  /**
   * Parses job and trigger configurations, preparing them for any code request of a matching job.
   * In our world, jobs and triggers are essentially 'merged'  - our scheduler is built around triggers
   * and jobs are basically 'idiot' programs who fire off messages.
    * 解析job和trigger的配置，为匹配作业的任何请求做好准备，在我们的世界中，作业和触发器基本上是“合并的” - 我们的调度程序是围绕触发器构建的，
    * 并且job基本上是一个只会发送消息的白痴程序
   *
   * RECAST KEY AS UPPERCASE TO AVOID RUNTIME LOOKUP ISSUES
   */

  /**
    * 返回了两个QuartzSchedule，
    * 一个是 QuartzSchedule 做任务的分发的，
    * 一个是 JobScheduler   生成任务计划表的，
    * 并构建好了他们各自的 trigger
    */
  var schedules: immutable.Map[String, QuartzSchedule] = QuartzSchedules(config, defaultTimezone).map { kv =>
    kv._1.toUpperCase -> kv._2
  }

  /**
    * runningJobs 中跑的就是上面返回的内个schedules 中的两个QuartzSchedules吗？ 现在看是的
    */
  val runningJobs: mutable.Map[String, JobKey] = mutable.Map.empty[String, JobKey]

  log.debug("Configured Schedules: {}", schedules)

  //启动 scheduler (在这启动是什么意思？ 还没有注册trigger把？)
  scheduler.start

  initialiseCalendars()  //暂时跳过

  /**
   * Puts the Scheduler in 'standby' mode, temporarily halting firing of triggers. 暂时停止触发，
   * Resumable by running 'start'   可用start恢复
   */
  def standby(): Unit = scheduler.standby()

  def isInStandbyMode = scheduler.isInStandbyMode

  /**
    * 启动scheduler，用于用户从 standby 状态启动，如果scheduler当前状态不是start，直接启动并返回true，如果当前状态已经是start了，则返回false
    * Starts up the scheduler. This is typically used from userspace only to restart a scheduler in standby mode.
    *
    * @return True if calling this function resulted in the starting of the scheduler; false if the scheduler
    *         was already started.
    */
  def start(): Boolean = isStarted match {
    case true =>
      log.warning("Cannot start scheduler, already started.")
      false
    case false =>
      scheduler.start
      true
  }

  def isStarted = scheduler.isStarted

  /**
   * Returns the next Date a schedule will be fired
    * 返回一个job的下一次触发时间
   */
  def nextTrigger(name: String): Option[Date] = {
    import scala.collection.JavaConverters._
    for {
      jobKey <- runningJobs.get(name)
      trigger <- scheduler.getTriggersOfJob(jobKey).asScala.headOption
    } yield trigger.getNextFireTime
  }

  /**
   * Suspends (pauses) all jobs in the scheduler
    * 暂停所有任务的调度
   */
  def suspendAll(): Unit = {
    log.info("Suspending all Quartz jobs.")
    scheduler.pauseAll()
  }

  /**
    * Shutdown the scheduler manually. The scheduler cannot be re-started.
    * 手动关闭scheduler，而且这个scheduler不能再次重启；
    *
    * @param waitForJobsToComplete wait for jobs to complete? default to false
    */
  def shutdown(waitForJobsToComplete: Boolean = false) = {
    scheduler.shutdown(waitForJobsToComplete)
  }

  /**
    * Attempts to suspend (pause) the given job
    * 暂停一个给定job，
    *
    * @param name The name of the job, as defined in the schedule
    * @return Success or Failure in a Boolean
    */
  def suspendJob(name: String): Boolean = {
    runningJobs.get(name) match {
      case Some(job) =>
        log.info("Suspending Quartz Job '{}'", name)
        scheduler.pauseJob(job)
        true
      case None =>
        log.warning("No running Job named '{}' found: Cannot suspend", name)
        false
    }
    // TODO - Exception checking?
  }

  /**
    * Attempts to resume (un-pause) the given job
    * 恢复一个给定job
    *
    * @param name The name of the job, as defined in the schedule
    * @return Success or Failure in a Boolean
    */
  def resumeJob(name: String): Boolean = {
    runningJobs.get(name) match {
      case Some(job) =>
        log.info("Resuming Quartz Job '{}'", name)
        scheduler.resumeJob(job)
        true
      case None =>
        log.warning("No running Job named '{}' found: Cannot unpause", name)
        false
    }
    // TODO - Exception checking?
  }

  /**
   * Unpauses all jobs in the scheduler
    * 恢复当前scheduler的所有job
   */
  def resumeAll(): Unit = {
    log.info("Resuming all Quartz jobs.")
    scheduler.resumeAll()
  }

  /**
    * Cancels the running job and all associated triggers
    * 从scheduler中取消job和所有相关的触发器
    *
    * @param name The name of the job, as defined in the schedule
    * @return Success or Failure in a Boolean
    */
  def cancelJob(name: String): Boolean = {
    runningJobs.get(name) match {
      case Some(job) =>
        log.info("Cancelling Quartz Job '{}'", name)
        val result = scheduler.deleteJob(job)
        runningJobs -= name
        result
      case None =>
        log.warning("No running Job named '{}' found: Cannot cancel", name)
        false
    }
    // TODO - Exception checking?
  }

  /**
   * Create a schedule programmatically (must still be scheduled by calling 'schedule')
    * 用代码的方式创建一个新的schedule（可以直接理解成一个Job）
   *
   * @param name A String identifying the job ：  job的标识符
   * @param description A string describing the purpose of the job ： job的描述(作用)
   * @param cronExpression A string with the cron-type expression ：  crontab表达式
   * @param calendar An optional calendar to use.： 一个可选的日历信息
   *
   */
  def createSchedule(name: String, description: Option[String] = None, cronExpression: String, calendar: Option[String] = None,
                     timezone: TimeZone = defaultTimezone) = schedules.get(name.toUpperCase) match {
    case Some(sched) =>
      throw new IllegalArgumentException(s"A schedule with this name already exists: [$name]")
    case None =>
      val expression = catching(classOf[ParseException]) either new CronExpression(cronExpression) match {
        case Left(t) =>
          throw new IllegalArgumentException(s"Invalid 'expression' for Cron Schedule '$name'. Failed to validate CronExpression.", t)
        case Right(expr) => expr
      }
      val quartzSchedule = new QuartzCronSchedule(name, description, expression, timezone, calendar)
      schedules += (name.toUpperCase -> quartzSchedule)    //仍然加到 schedules 中，里面本身就有两个了，一个做任务分发的，一个做生成执行计划的
  }

  /**
    * Reschedule a job
    * 重新调度一个job
    *
    * @param name           A String identifying the job： job的标识符
    * @param receiver       An ActorRef, who will be notified each time the schedule fires ：  每次scheduler(的trigger)触发的时候，这个receiver都会收到通知
    * @param msg            A message object, which will be sent to `receiver` each time the schedule fires：  每次scheduler触发的时候，发给receiver的通知消息
    * @param description    A string describing the purpose of the job    job的描述
    * @param cronExpression A string with the cron-type expression      cron 表达式
    * @param calendar       An optional calendar to use.                需要排除的节假日
    * @return A date which indicates the first time the trigger will fire.   返回这个 scheduler(trigger) 第一次触发的日期
    */

  def rescheduleJob(name: String, receiver: ActorRef, msg: AnyRef, description: Option[String] = None,
                    cronExpression: String, calendar: Option[String] = None, timezone: TimeZone = defaultTimezone): Date = {
    cancelJob(name)  //取消job
    removeSchedule(name)  //删除scheduler
    createSchedule(name, description, cronExpression, calendar, timezone)  //创建新的scheduler
    scheduleInternal(name, receiver, msg, None)       //重新在内部调度新的scheduler (job)
  }

  private def removeSchedule(name: String) = schedules = schedules - name.toUpperCase

  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 ActorRef， 需要立即执行的
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An ActorRef, who will be notified each time the schedule fires
    * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: ActorRef, msg: AnyRef): Date = scheduleInternal(name, receiver, msg, None)


  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 ActorSelection，需要立即执行的
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An ActorSelection, who will be notified each time the schedule fires
    * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: ActorSelection, msg: AnyRef): Date = scheduleInternal(name, receiver, msg, None)


  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 EventStream，需要立即执行的
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An EventStream, who will be published to each time the schedule fires
    * @param msg      A message object, which will be published to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: EventStream, msg: AnyRef): Date = scheduleInternal(name, receiver, msg, None)

  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 ActorRef， 需要设置开始执行时间的，
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An ActorRef, who will be notified each time the schedule fires
    * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: ActorRef, msg: AnyRef, startDate: Option[Date]): Date = scheduleInternal(name, receiver, msg, startDate)


  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 ActorSelection，需要设置开始执行时间的，
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An ActorSelection, who will be notified each time the schedule fires
    * @param msg      A message object, which will be sent to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: ActorSelection, msg: AnyRef, startDate: Option[Date]): Date = scheduleInternal(name, receiver, msg, startDate)

  /**
    * Schedule a job, whose named configuration must be available
    * receiver是一个 EventStream，需要设置开始执行时间的
    * @param name     A String identifying the job, which must match configuration
    * @param receiver An EventStream, who will be published to each time the schedule fires
    * @param msg      A message object, which will be published to `receiver` each time the schedule fires
    * @return A date which indicates the first time the trigger will fire.
    */
  def schedule(name: String, receiver: EventStream, msg: AnyRef, startDate: Option[Date]): Date = scheduleInternal(name, receiver, msg, startDate)

  /**
    * Helper method for schedule because overloaded methods can't have default parameters.
    * 开始在内部调度，
    * @param name The name of the schedule / job:               schedule / job 的名字
    * @param receiver The receiver of the job message. This must be either an ActorRef or an ActorSelection.：   job消息的接受者，必须是一个ActorRef或者ActorSelection
    * @param msg The message to send to kick off the job.:       每次scheduler(trigger)触发的时候，发送给receiver的消息
    * @param startDate The optional date indicating the earliest time the job may fire.     第一次触发的时间
    * @return A date which indicates the first time the trigger will fire.：       第一次触发的时间
    */
  private def scheduleInternal(name: String, receiver: AnyRef, msg: AnyRef, startDate: Option[Date]): Date = schedules.get(name.toUpperCase) match {
    case Some(schedule) => scheduleJob(name, receiver, msg, startDate)(schedule)
    case None => throw new IllegalArgumentException("No matching quartz configuration found for schedule '%s'".format(name))
  }


  /**
   * Creates the actual jobs for Quartz, and setups the Trigger, etc.
   * 为quartz创建实际的job，并配置trigger等信息，
    * ---------两个配置的job任务(scheduler)真正被执行的地方-----------
   * @return A date, which indicates the first time the trigger will fire.:  返回第一次触发的日期
   */
  protected def scheduleJob(name: String, receiver: AnyRef, msg: AnyRef, startDate: Option[Date])(schedule: QuartzSchedule): Date = {
    import scala.collection.JavaConverters._
    log.info("Setting up scheduled job '{}', with '{}'", name, schedule)
    val jobDataMap = Map[String, AnyRef](  //创建 jobDataMap
      "logBus" -> system.eventStream,
      "receiver" -> receiver,
      "message" -> msg
    )

    val jobData = JobDataMapSupport.newJobDataMap(jobDataMap.asJava)
    val job = JobBuilder.newJob(classOf[SimpleActorMessageJob])
      .withIdentity(name + "_Job")   //只指定了一个string，说明group设置的是null
      .usingJobData(jobData)
      .withDescription(schedule.description.orNull)
      .build()

    log.debug("Adding jobKey {} to runningJobs map.", job.getKey)

    runningJobs += name -> job.getKey  //jobKey ： ${jobname}_Job  ()

    log.debug("Building Trigger with startDate '{}", startDate.getOrElse(new Date()))
    val trigger = schedule.buildTrigger(name, startDate)

    log.debug("Scheduling Job '{}' and Trigger '{}'. Is Scheduler Running? {}", job, trigger, scheduler.isStarted)
    scheduler.scheduleJob(job, trigger)  //开始调度
  }


  /**
   * Parses calendar configurations, creates Calendar instances and attaches them to the scheduler
    * 解析要排除的节假日信息，并注册到scheduler上，[先跳过这个不看了]
   */
  protected def initialiseCalendars() {
    for ((name, calendar) <- QuartzCalendars(config, defaultTimezone)) {
      log.info("Configuring Calendar '{}'", name)
      // Recast calendar name as upper case to make later lookups easier ( no stupid case clashing at runtime )
      scheduler.addCalendar(name.toUpperCase, calendar, true, true)
    }
  }


  /**
    * 创建一个线程池  (quartz 的api)
    */
  lazy protected val threadPool = {
    // todo - wrap one of the Akka thread pools with the Quartz interface?
    val _tp = new SimpleThreadPool(threadCount, threadPriority)
    _tp.setThreadNamePrefix("AKKA_QRTZ_") // todo - include system name?
    _tp.setMakeThreadsDaemons(daemonThreads_?)
    _tp
  }

  /**
    * 内存存储？ no
    */
  lazy protected val jobStore: JobStore = {
    // TODO - Make this potentially configurable,  but for now we don't want persistable jobs.
    new RAMJobStore()
  }

  /**
    * 真正的scheduler
    */
  lazy protected val scheduler = {
    // because it's a java API ... initialize the scheduler, THEN get and start it.
    // schedulerName:  QuartzScheduler~${system.name}
    // schedulerInstanceId: ${system.name}
    // threadPool、jobStore
    DirectSchedulerFactory.getInstance.createScheduler(schedulerName, system.name, /* todo - will this clash by quartz' rules? */
      threadPool, jobStore)

    val scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName)   // 获取scheduler

    log.debug("Initialized a Quartz Scheduler '{}'", scheduler)

    //actor 终止的时候停止调度
    system.registerOnTermination({
      log.info("Shutting down Quartz Scheduler with ActorSystem Termination (Any jobs awaiting completion will end as well, as actors are ending)...")
      scheduler.shutdown(false)
    })

    scheduler
  }

}
