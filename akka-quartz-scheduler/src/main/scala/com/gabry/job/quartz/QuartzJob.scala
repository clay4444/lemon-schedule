package com.gabry.job.quartz

import akka.actor.{ActorRef, ActorSelection}
import akka.event.{EventStream, Logging, LoggingBus}
import org.quartz.{Job, JobDataMap, JobExecutionContext, JobExecutionException}

/**
 * Base trait, in case we decide to diversify down the road
 * and allow users to pick "types" of jobs, we still want
 * strict control over them monkeying around in ways that
 * exposes the "bad" parts of Quartz –
 * such as persisted mutable state
 */
sealed trait QuartzJob extends Job {
  def jobType: String

  /**
   * Fetch an item, cast to a specific type, from the JobDataMap.
   * I could just use apply, but I want to have a cleaner 'not there' error.
   *
   * This does not return Option and flatly explodes upon a key being missing.
   * 从jobDataMap中找到一个指定类型的数据，没有找的的时候抛出异常，而不是返回Option
   * TODO - NotNothing check?
   **/
  protected def as[T](key: String)(implicit dataMap: JobDataMap): T = Option(dataMap.get(key)) match {
    case Some(item) => item.asInstanceOf[T]
    case None => throw new NoSuchElementException("No entry in JobDataMap for required entry '%s'".format(key))
  }

  /**
   * Fetch an item, cast to a specific type, from the JobDataMap.
   * I could just use apply, but I want to have a cleaner 'not there' error.
   * 和上面不一样吗？
   * TODO - NotNothing check?
   **/
  protected def getAs[T](key: String)(implicit dataMap: JobDataMap): Option[T] = Option(dataMap.get(key)).map(_.asInstanceOf[T])
}


/**
  * quartz Job的实现类
  */
class SimpleActorMessageJob extends Job {
  val jobType = "SimpleActorMessage"

  /**
   * Fetch an item, cast to a specific type, from the JobDataMap.
   * I could just use apply, but I want to have a cleaner 'not there' error.
   *
   * This does not return Option and flatly explodes upon a key being missing.
   * 又写一遍？
   * TODO - NotNothing check?
   **/
  protected def as[T](key: String)(implicit dataMap: JobDataMap): T = Option(dataMap.get(key)) match {
    case Some(item) => item.asInstanceOf[T]
    case None => throw new NoSuchElementException("No entry in JobDataMap for required entry '%s'".format(key))
  }

  /**
   * Fetch an item, cast to a specific type, from the JobDataMap.
   * I could just use apply, but I want to have a cleaner 'not there' error.
   * ...
   * TODO - NotNothing check?
   **/
  protected def getAs[T](key: String)(implicit dataMap: JobDataMap): Option[T] = Option(dataMap.get(key)).map(_.asInstanceOf[T])

  /**
   * These jobs are fundamentally ephemeral - a new Job is created
   * each time we trigger, and passed a context which contains, among
   * other things, a JobDataMap, which transfers mutable state
   * from one job trigger to another
   * 这些工作基本上都是非常短暂的 - 每次触发时都会创建一个新的Job，并传递一个上下文，其中包含一个JobDataMap，它将可变状态从一个作业触发器转移到另一个作业触发器
    *
   * @throws JobExecutionException
   */
  def execute(context: JobExecutionContext) {
    implicit val dataMap = context.getJobDetail.getJobDataMap
    val key  = context.getJobDetail.getKey

    try {
      val logBus = as[LoggingBus]("logBus")
      val receiver = as[AnyRef]("receiver")

      /**
       * Message is an instance, essentially static, not a class to be instantiated.
       * JobDataMap uses AnyRef, while message can be any (though do we really want to support value classes?)
       * so this casting (and the initial save into the map) may involve boxing.
       **/
      val msg = dataMap.get("message") match {
        case MessageRequireFireTime(m) =>
          MessageWithFireTime(m,context.getScheduledFireTime)
        case any:Any => any
      }
      val log = Logging(logBus, this)
      log.debug("Triggering job '{}', sending '{}' to '{}'", key.getName, msg, receiver)
      receiver match {
        case ref: ActorRef => ref ! msg
        case selection: ActorSelection => selection ! msg
        case eventStream: EventStream => eventStream.publish(msg)
        case _ => throw new JobExecutionException("receiver as not expected type, must be ActorRef or ActorSelection, was %s".format(receiver.getClass))
      }
    } catch {
      // All exceptions thrown from a job, including Runtime, must be wrapped in a JobExcecutionException or Quartz ignores it
      case jee: JobExecutionException => throw jee
      case t: Throwable =>
        throw new JobExecutionException("ERROR executing Job '%s': '%s'".format(key.getName, t.getMessage), t) // todo - control refire?
    }
  }
}
