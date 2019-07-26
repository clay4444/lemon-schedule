package com.gabry.job.scheduler.test

import java.text.SimpleDateFormat
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoField
import java.util.Date

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser


object testCron extends App {

  private val cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
  private val parser = new CronParser(cronDefinition)
  private val zoneIdKey = "Asia/Shanghai"
  private val zoneId = ZoneId.of(zoneIdKey)

  val crontab:String = "0/30 * * * *"
  val lastTriggerTime:Long = 1562455800000L


  getNextTriggerTime(crontab,lastTriggerTime) match {

    case Some(nextTriggerTime) => {
      val format0 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      val time = format0.format(nextTriggerTime);
      println(time)
    }
    case None => println("can not generate next trigger time")
  }


  def getNextTriggerTime(cronExpression:String, timeAfter:Long):Option[Long] = {
    getNextTriggerTime(cronExpression,ZonedDateTime.ofInstant(Instant.ofEpochMilli(timeAfter), zoneId))}

  private def getNextTriggerTime(cronExpression:String,timeAfter:ZonedDateTime):Option[Long] = {
    try {
      val cron = parser.parse(cronExpression)
      val executionTime = ExecutionTime.forCron(cron)
      val javaOptionalDate = executionTime.nextExecution(timeAfter)
      if(javaOptionalDate.isPresent){
        val nextExecution = javaOptionalDate.get()
        Some(nextExecution.getLong(ChronoField.INSTANT_SECONDS)*1000+nextExecution.getLong(ChronoField.MILLI_OF_SECOND))
      }
      else None
    }catch {
      case ex:Exception =>
        println(ex.toString)
        None
    }
  }
}
