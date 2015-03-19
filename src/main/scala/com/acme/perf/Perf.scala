package com.acme.perf

import com.acme.Util
import com.acme.Env
import com.acme.aws.S3
import scala.util.matching.Regex

import org.apache.spark.SparkContext

trait Perf extends S3 {
  val runAt = Util.utcNow
  
  protected def setS3Credentials(sc: SparkContext) : Unit = {
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", S3Key)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", S3Secret)
  }
  
  protected case class CliConfig(
    date: String = "",
    rate: Double = 1,
    s3Prefix: String = ""
  )

  protected def parseClArgs(args: Array[String]) : Option[CliConfig] = {
    val dateRegex = new Regex("(\\d{4})-(\\d{2})-(\\d{2})")
    
    val parser = new scopt.OptionParser[CliConfig]("scopt") {
      opt[String]('d', "date") action { (x, c) =>
        c.copy(date = x)
      } validate { x =>
         x match {
           case dateRegex(year, month, date) => success
           case _ => failure("env must be valid")
         }
      } text ("Date should be in the format of YYYY-MM-DD")

      opt[Int]("rate") action { (x, c) =>
        c.copy(rate = x)
      } text ("rate in the read rate e.g. 100 items/sec")

      opt[String]("s3Prefix") action { (x, c) =>
        c.copy(s3Prefix = x)
      } text ("s3 prefix for input files")

      help("help") text ("prints this usage text")
    }

    parser.parse(args, CliConfig())
  }

}