package com.acme

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import com.acme.codec.UserUpdate

object UserLog {

  def main(args: Array[String]) {

    def parseClArgs(args: Array[String]) = {
      if (args.size < 2) {
        println("Usage: UserLog <in_folder> <out_folder>")
        System.exit(0)
      }
      val inFolder = args(0)
      val outFolder = args(1)
      (inFolder, outFolder)
    }

    val env = "development"
    val (inFolder, outFolder) = parseClArgs(args)
    val appConfig = if (env == Env.production)
      Config(env, inFolder = s"s3n://$inFolder", outFolder = s"s3n://$outFolder")
    else
      Config(env, inFolder = s"$inFolder", outFolder = s"$outFolder")

    Util.dump(appConfig.toString)

    val conf = new SparkConf()
      .setAppName("RtUserlog")

    val sc = new SparkContext(conf)
    var total = 0

    val lines = sc.textFile(appConfig.inFolder)
    val result = lines.map(UserUpdate.decode)      
      .filter(_.isDefined)
      .flatMap {
        userUpdate =>
          {
            val uu = userUpdate.get
            val (userId, lastUpdate, dps) = (uu.userId, uu.lastUpdate, uu.data)
            val userDps = for {
              dp <- dps
            } yield (userId, dp)
            userDps
          }
      }
      .map(userAndDp => (userAndDp._2, userAndDp._1))
      .sortByKey(true)
      .saveAsTextFile(appConfig.outFolder)

    sc.stop()
  }
}
