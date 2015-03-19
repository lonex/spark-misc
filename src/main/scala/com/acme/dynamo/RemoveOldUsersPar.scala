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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object RemoveOldUsersPar {

  val limit = 4000 // each segment has limit items
  val exportPrefix = "d_"
  val filePattern = new Regex(s"(${exportPrefix})(\\d[0-9]+)_(\\d[0-9]+)")
  val maxSegments = 1000*1000 // AWS maximum
  val runAt = Util.utcNow

  def main(args: Array[String]) {
    val conf = parseClArgs(args)
    if (conf.isEmpty) System.exit(1)

    val (env, region, windowSize, rate, oPath, nrSegments, split) = 
      (conf.get.env, conf.get.region, conf.get.windowSize, conf.get.rate, conf.get.outPath, conf.get.nrSegments, conf.get.split)

    val totalSegments = math.min((User.itemCount(env) / limit).toInt, maxSegments)
    val segments = util.Random.shuffle((0 until totalSegments).toList).take(nrSegments)
    
    val start = System.currentTimeMillis
    
    println(s"> Env [${env}] Region [${region}] Table [${User.TABLE}] Item count ${User.itemCount(env)} Segment size $limit")
    println(s"> Rate controlled by Read at $rate items/sec, approx totalSegments: $totalSegments, processing $nrSegments segments this time")
    println(s"> Delete users older than $windowSize days")
    println(s"> Spark RDD parallelism split $split")
    
    val summaryFile = List(outputFolder(oPath, region), "summary").mkString("/")
    
    val sparkConf = new SparkConf()
      .setAppName("RemoveOldUsers")

    val sc = new SparkContext(sparkConf)
    val totalAccumulator = sc.accumulator(0, "Total Deleted")
    
    var nr = sc.parallelize(segments, split).map { segment => 
      val rateControl = new RateControl
      implicit val dynamoDB = new com.acme.dynamo.Db(Config(env, region)).client
      val usersTable = User.tableRef

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
      if (users.size > 0) {
        deleteUsers(users, dynamoDB)
        saveUsers(outputFolder(oPath, region), segment, users)
      }
      
      val delta = System.currentTimeMillis - start_time
      
      totalAccumulator += users.size
      rateControl.execute(rate, limit, delta)

      users.size
    }.reduce(_ + _)
    
    println(" = total accumulator " + totalAccumulator.value)
    println(" = rate " + totalAccumulator.value / ((System.currentTimeMillis() - start) /1000.0) )
    println(" = Deleted total " + nr)
    
    sc.stop()
  }

  
  def deleteUsers(users : ArrayBuffer[User], dynamoDB : DynamoDB) : Int = {
    var total = 0
    users foreach {
      user => 
        total += 1
        User.delete(user)(dynamoDB)
    }
    
    total
  }
  
  def saveSummary(file: String, total: Long) = {
    val path = new File(file)
    if (!path.exists)
      path.createNewFile

    val fileWriter = new FileWriter(path, false)
    val bw = new BufferedWriter(fileWriter)
    bw.write("Total deleted " + total + "\n")
    bw.close
  }
  
  def saveUsers(folder: String, segment: Int, users: ArrayBuffer[User]) : Unit = {
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
    nrSegments: Int = 0, 
    rate: Double = User.READ_CAP,
    split: Int = 2*2*8)

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

      opt[Int]("nrSegments") action { (x, c) =>
        c.copy(nrSegments = x)
      } text ("Total nr of segments to scan for the job")

      opt[Int]("split") action { (x, c) =>
        c.copy(split = x)
      } text ("Total nr of splits for Spark RDD")

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

  def outputFolder(oPath: String, region: String) = List(oPath, region).mkString("/")
    
  def segmentFile(segment: Int) : String = 
    exportPrefix + "%07d".format(segment)
    
}
