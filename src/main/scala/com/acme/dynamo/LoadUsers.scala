package com.acme.dynamo

import awscala._
import awscala.dynamodbv2._
import com.acme.Util

object LoadUsers {

  def main(args: Array[String]) {
    val (nrItems, env) = parseClArgs(args)

    val conf = if (env == com.acme.Env.production)
      Config(com.acme.Env.production)
    else
      Config(com.acme.Env.development)

    println("> " + env)

    implicit val dynamoDB = new com.acme.dynamo.Db(conf).client
    val userTable = User.createTable

    val tsGen = new UnixTsGenerator
    var items = 0
    var batch = 1000
    val rate : Long = 1000 / User.WRITE_CAP // 1000 milli-sec for 100 items
    var totalTime : Long = 0
    
    for (i <- 0 until nrItems) {
      val start_time = System.currentTimeMillis
      val item = User(nextUUID, tsGen.next.toInt)
      val r = User.insert(userTable, item)
      items += 1
      val end_time = System.currentTimeMillis
      val delta = rate - (end_time - start_time)  
      rateControl(rate, end_time - start_time)
      totalTime += end_time - start_time
      
      if (items % batch == 0) {
        println(s"Loaded $items items, took ${totalTime/1000.0} secs")
        totalTime = 0L
      }
    }
  }
  
  def rateControl(rate: Long, used: Long) = {
	if (rate - used > 0)
	  Thread.sleep(rate - used)
  }
  
  def parseClArgs(args: Array[String]) = {
    if (args.size < 1) {
      println("Usage: LoadUsers <nr_of_items> [env]")
      System.exit(0)
    }
    val nrItems = args(0).toInt
    val env = if (args.size < 2) com.acme.Env.development
    else args(1)
    (nrItems, env)
  }

  class UnixTsGenerator {
    val rnd = new scala.util.Random
    val start = Util.dateAndTimeStringsToTimestamp("2014-01-01", "00:00:00")
    val end = Util.dateAndTimeStringsToTimestamp("2015-01-31", "23:59:59")
    val range = start to end

    def next = range(rnd.nextInt(range length))
  }

  def nextUUID = java.util.UUID.randomUUID.toString

}