package com.acme.dynamo

import com.acme.Util
import com.acme.Env

import awscala._
import awscala.dynamodbv2._
import java.io._
import util.matching.Regex
import util.control.Breaks._
import org.joda.time.DateTime;
import collection.mutable.ArrayBuffer

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef

object RemoveOldUsers {

  val limit = 8000 // each segment has limit items
  val exportPrefix = "d_"
  val filePattern = new Regex(s"(${exportPrefix})(\\d[0-9]+)_(\\d[0-9]+)")
  val maxSegments = 1000*1000 // AWS maximum
  var shutdownOk = true
  val runAt = Util.utcNow
  
  def main(args: Array[String]) {
    val conf = parseClArgs(args)
    if (conf.isEmpty) System.exit(1)

    val (env, region, windowSize, rate, oPath, startSegment) = 
      (conf.get.env, conf.get.region, conf.get.windowSize, conf.get.rate, conf.get.outPath, conf.get.segment)

    var total = 0L
    var totalTime: Long = 0

    val totalSegments = math.min((User.itemCount(env) / limit).toInt, maxSegments)
    val segments = util.Random.shuffle((0 until totalSegments).toList)
    
    val start = System.currentTimeMillis
    
    println(s"> Env [${env}] Region [${region}] Table [${User.TABLE}] Item count ${User.itemCount(env)} Segment size $limit")
    println(s"> Rate controlled by Read at $rate items/sec, totalSegments: $totalSegments")
    println(s"> Delete users older than $windowSize days")
    println(s"> Starts at $runAt")

    implicit val dynamoDB = new com.acme.dynamo.Db(Config(env, region)).client
    val usersTable = User.tableRef
    if (!usersTable.isDefined) {
      println("Table not defined")
      System.exit(1)
    }

    val shutdownThread = sys.addShutdownHook(shutdown)

    val rateControl = new RateControl
   
    for (segment <- segments) {
      val cutOff = daysAgo(windowSize)
      var start_time = System.currentTimeMillis
      val items: Seq[Item] = usersTable.get.scan(
        filter = Seq(User.LAST_UPDATE -> Condition.lt(cutOff)),
        limit = limit,
        segment = segment,
        attributesToGet = Seq(User.USER_ID, User.LAST_UPDATE),
        select = com.amazonaws.services.dynamodbv2.model.Select.SPECIFIC_ATTRIBUTES,
        totalSegments = totalSegments)

      val users = parseResult(items, segment)
      deleteUsersInPar(users, segment)
      total += users.size
      
      val delta = System.currentTimeMillis - start_time
      if (users.size > 0) 
        saveUsers(outputFolder(oPath, region), segment, users)

      val secs = (System.currentTimeMillis - start) / 1000.0
      println(s"Total $total items, took ${secs} sec, approx rate ${total / 1.0 / secs} items/sec")
        
      rateControl.execute(rate, limit, delta)
    }

    
    shutdownThread.run
  }

  def deleteUsers(users : ArrayBuffer[User])(implicit dynamoDB : DynamoDB) : Int = {
    var total = 0
    users foreach {
      user => 
        total += 1
        User.delete(user)
    }
    
    total
  }
  
  val akkaConf = com.acme.Config.appConf

  def deleteUsersInPar(users: ArrayBuffer[User], segment: Int)(implicit dynamoDB : DynamoDB) = {
    val akka = ActorSystem("acme", akkaConf.getConfig("acme-akka"))
    val runner = akka.actorOf(Props(new UserDeleteRunner(users, segment, dynamoDB)), "user-delete-runner")
  }
  
  def saveUsers(folder: String, segment: Int, users: ArrayBuffer[User]) : Unit = {
      shutdownOk = false
      
      val dir = new File(folder)
      if (!dir.exists)
        dir.mkdirs

      val path = new File(List(folder, segmentFile(segment)).mkString("/"))
      if (!path.exists)
        path.createNewFile

      val fileWriter = new FileWriter(path, false)
      val bw = new BufferedWriter(fileWriter)
      println(s"[${Util.utcNow}] Saving ${users.size} deleted user to $path ...")

      users foreach {
        user => bw.write(user.user_id + "," + user.last_update + "\n")
      }
      bw.close
      shutdownOk = true
  }

  def parseResult(items: Seq[Item], segment: Int): ArrayBuffer[User] = {
    val users = ArrayBuffer.empty[User]

    val iter = items.iterator
    var total = 0
    var lastItem: Option[User] = None

    while (iter.hasNext) {
      User.decode(iter.next.attributes).foreach{
        user => {
          users += user
          lastItem = Some(user)
        }
      }

      total += 1
    }

    println(s"Segment $segment, total $total users, last item " + lastItem.getOrElse("Error"))
    users
  }

  case class CliConfig(env: String = Env.development,
    region: String = "US_WEST_2",
    windowSize: Int = 7 * 30, // 7 month
    outPath: String = "/tmp",
    segment: Int = 0, 
    rate: Double = User.READ_CAP)

  def parseClArgs(args: Array[String]) : Option[CliConfig] = {
    val parser = new scopt.OptionParser[CliConfig]("scopt") {
      opt[String]('e', "env") action { (x, c) =>
        c.copy(env = x)
      } validate { x =>
        if (Env.envs.contains(x)) success else failure("env must be valid")
      } text ("env is production or development")

      opt[String]('r', "region") action { (x, c) =>
        c.copy(region = x)
      } validate { x =>
        if (Db.Regions.contains(x)) success else failure("region must be valid")
      } text (s"region is one of ${Db.Regions.mkString(", ")}")

      opt[Int]("windowSize") action { (x, c) =>
        c.copy(windowSize = x)
      } text ("retention windowSize in days, e.g. 180 days")

      opt[Int]("segment") action { (x, c) =>
        c.copy(segment = x)
      } text ("start scan from this segment")

      opt[Int]("rate") action { (x, c) =>
        c.copy(rate = x)
      } text ("rate in the read rate e.g. 100 items/sec")

      opt[String]("outPath") action { (x, c) =>
        c.copy(outPath = x)
      } text ("outPath is the folder to save the user data")

      help("help") text ("prints this usage text")
    }

    parser.parse(args, CliConfig())
  }

  def daysAgo(days: Int) : Long =
    Util.utcNow.minusDays(days).getMillis() / 1000
 
  def outputFolder(oPath: String, region: String) = {
    val formatter = org.joda.time.format.DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm-ss")
    List(oPath, region, formatter.print(runAt)).mkString("/")
  }
    
  def segmentFile(segment: Int) : String = 
    exportPrefix + "%07d".format(segment)
    
  private def shutdown = {
    while(!shutdownOk) 
      Thread.sleep(1)
      
    println("Quiting...")
  }
}
