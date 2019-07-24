package com.gabry.job.quartz

import java.text.ParseException
import java.util.{Date, TimeZone}

import com.typesafe.config.{Config, ConfigException, ConfigObject}
import org.quartz._

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.util.control.Exception._

/**
 * This is really about triggers - as the "job" is roughly defined in the code that refers to the trigger.
 * 这是真正关于触发器的程序 - 因为 job 大致在引用触发器的代码中定义。
  *
 *  I call them Schedules to get people not thinking about Quartz in Quartz terms (mutable jobs, persistent state)
 *
 *  All jobs "start" immediately.
 */
object QuartzSchedules {
  // timezone (parseable) [optional, defaults to UTC]

  // calendars = 需要排除的日历的名称列表
  // calendars = list of calendar names that "modify" this schedule

  // description = 对于job的一个可选的描述
  // description = an optional description of the job [string] [optional]

  // expression = 符合quartz的规则的cron 表达式
  // expression = cron expression complying to Quartz' Cron Expression rules.
  // TODO - Misfire Handling

  val catchMissing = catching(classOf[ConfigException.Missing])       //缺少配置异常
  val catchWrongType = catching(classOf[ConfigException.WrongType])  //配置类型错误异常
  val catchParseErr = catching(classOf[ParseException])             //解析配置异常

  /**
    * 返回一个map，key是scheduler的name，value是对应的schedule，也就是说：并不是我想的那样，每个用户的job都由quartz直接调度，而是只有两个scheduler：quartz只调度两个job，
    * JobTaskDispatcher：  每分钟调度分发器进行任务的分发，
    * JobScheduler：       每分钟调度器生成任务计划表，
    * 分发的频率不能比调度的高，也就是说分发要尽量快？
    */
  def apply(config: Config, defaultTimezone: TimeZone): immutable.Map[String, QuartzSchedule] = catchMissing opt {
    /** The extra toMap call is because the asScala gives us a mutable map... */
    config.getConfig("schedules").root.asScala.toMap.flatMap {
      case (key, value: ConfigObject) =>
        Some(key -> parseSchedule(key, value.toConfig, defaultTimezone))
      case _ =>
        None
    }
  } getOrElse immutable.Map.empty[String, QuartzSchedule]

  /**
    * 解析 config 获取 scheduler
    * @param name
    * @param config   配置
    * @param defaultTimezone  时区
    * @return
    */
  def parseSchedule(name: String, config: Config, defaultTimezone: TimeZone): QuartzSchedule = {
    // parse common attributes
    val timezone = catchMissing opt {
      TimeZone.getTimeZone(config.getString("timezone")) // todo - this is bad, as Java silently swaps the timezone if it doesn't match...
    } getOrElse defaultTimezone

    val calendar = catchMissing opt {
      Option(config.getString("calendar")) // TODO - does Quartz validate for us that a calendar referenced is valid/invalid?
    } getOrElse None

    val desc = catchMissing opt {
      config.getString("description")
    }

    parseCronSchedule(name, desc, config)(timezone, calendar)
  }

  /**
    * 解析 config 获取 scheduler
    * @param name  名字？ 什么的名字？ scheduler 的名字 / 也可以理解为job的名字
    * @param desc  描述   scheduler 的描述？
    * @param config 配置
    * @param tz     时区
    * @param calendar  需要排除的节假日
    * @return
    */
  def parseCronSchedule(name: String, desc: Option[String], config: Config)(tz: TimeZone, calendar: Option[String]): QuartzCronSchedule = {
    val expression = catchMissing or catchWrongType either { config.getString("expression") } match {
      case Left(t) =>
        throw new IllegalArgumentException("Invalid or Missing Configuration entry 'expression' for Cron Schedule '%s'. You must provide a valid Quartz CronExpression.".format(name), t)
      case Right(str) => catchParseErr either new CronExpression(str) match {
        case Left(t) =>
          throw new IllegalArgumentException("Invalid 'expression' for Cron Schedule '%s'. Failed to validate CronExpression.".format(name), t)
        case Right(expr) => expr
      }
    }
    //创建真正的scheduler
    new QuartzCronSchedule(name, desc, expression, tz, calendar)
  }
}

/**
  * scheduler 接口
  */
sealed trait QuartzSchedule {
  type T <: Trigger

  def name: String

  def description: Option[String]

  // todo - I don't like this as we can't guarantee the builder's state, but the Quartz API forces our hand
  def schedule: ScheduleBuilder[T]

  //The name of the optional exclusion calendar to use. 要排除的节假日的名字
  //NOTE: This formerly was "calendars" but that functionality has since been removed as Quartz never supported more than one calendar anyways.
  //这个功能被取消了？ 因为quartz 不支持指定多个节假日？
  def calendar: Option[String]

  /**
    * 构建一个trigger，触发器
    * Utility method that builds a trigger with the data this schedule contains, given a name.
    * Job association can happen separately at schedule time.
    *
    * @param name The name of the job / schedule.
    * @param futureDate The Optional earliest date at which the job may fire. 开始执行的日期
    * @return The new trigger instance.
    */
  def buildTrigger(name: String, futureDate: Option[Date] = None): T = {
    val partialTriggerBuilder = TriggerBuilder.newTrigger()
                           .withIdentity(name + "_Trigger")
                           .withDescription(description.orNull)
                           .withSchedule(schedule)
                           
    var triggerBuilder = futureDate match {
      case Some(fd) => partialTriggerBuilder.startAt(fd)
      case None => partialTriggerBuilder.startNow()
    }

    triggerBuilder = calendar.map(triggerBuilder.modifiedByCalendar).getOrElse(triggerBuilder)
    triggerBuilder.build()
  }

}

/**
  * 真正的 CronScheduler
  * @param name
  * @param description
  * @param expression
  * @param timezone
  * @param calendar
  */
final class QuartzCronSchedule(val name: String,
                               val description: Option[String] = None,
                               val expression: CronExpression,
                               val timezone: TimeZone,
                               val calendar: Option[String] = None) extends QuartzSchedule {

  type T = CronTrigger

  // Do *NOT* build, we need the uncompleted builder. I hate the Quartz API, truly.
  //创建一个CronScheduleBuilder,为什么需要一个未完成的builder呢？
  val schedule = CronScheduleBuilder.cronSchedule(expression).inTimeZone(timezone)
}

