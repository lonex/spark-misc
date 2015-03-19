package com.acme

import org.joda.time._
import org.joda.time.format.DateTimeFormatter
import org.joda.time.format.DateTimeFormat

object Util {
  val PREFIX = " "
  val fileStringPattern = """(.*event\.log)_(?<year>\d{4})-(?<month>\d\d)-(?<date>\d\d)-(?<hour>\d\d)-(?<ts>\d+)_(?<host>[^.]+)\.([^.]*)""".r
  val timeFormatter  = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZoneUTC
  val timeFormatter2 = DateTimeFormat.forPattern("yyyy:MM:dd HH:mm:ss").withZoneUTC
  
  def dump(msg: String) = println(PREFIX + msg)

  def getOrElse(propExists: Boolean, v: Int, default: Int) = if (propExists) v else default

  def isBlank(o: Option[String]) = o.isEmpty || o.filter(_.trim.length > 0).isEmpty

  def extractHost(filename: String): String = {
    filename match {
      case fileStringPattern(_, year, month, date, hour, ts, host, _*) => host
      case _ => filename
    }
  }

  def dateAndTimeStringsToTimestamp(dateStr: String, timeStr: String): Long = {
    val dateTime = timeFormatter.parseDateTime(dateStr + " " + timeStr)
    dateTime.getMillis / 1000L
  }
  
  def stringToDateTime(s: String) : DateTime = {
    DateTime.parse(s, timeFormatter2)
  }
  
  def utcNow : DateTime = new DateTime(DateTimeZone.UTC)

  def stripWhitespace(str: String) = {
    val whitespaces = """(\r|\n|\t)""".r
    whitespaces replaceAllIn (str, "")
  }
}