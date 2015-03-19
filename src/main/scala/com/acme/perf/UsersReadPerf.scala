package com.acme.perf

import com.acme.Util
import com.acme.Env

import util.matching.Regex
import org.joda.time.DateTime;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.acme.codec.warehouse.Track
import com.acme.codec.warehouse.Track._
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * Performance test of reading data from Cassandra users CF.
 * 
 * To run in Spark standalone mode 
 * 
 * /path/to/spark-submit 
 *   --class com.acme.perf.UsersReadPerf 
 *   --master local[1] 
 *   --driver-java-options "-Denv=development -Dapp.conf=src/main/resources/application.conf" 
 *   --conf "spark.executor.extraJavaOptions=-Denv=development -Dapp.conf=src/main/resources/application.conf" 
 *   target/scala-2.10/extract_0.0.1.jar  --date 2014-06-11
 *   
 * Due to the way how Spark driver loads JVM option, and the executor's classpath setting, and customized settings 
 * are not passed to the executor
 * from the driver, you need to specify the configuration and environment variable using --driver-java-options 
 * and spark.executor.extraJavaOptions.
 */
object UsersReadPerf extends Perf {

  private[this] case object Lock
  private var nrSuccess : Long = 0
  private var nrFailure : Long = 0
  
  def main(args: Array[String]) {
    val conf = parseClArgs(args)
    if (conf.isEmpty) System.exit(1)

    val (date, rate, s3Prefix) = (conf.get.date, conf.get.rate, conf.get.s3Prefix)
    val start = System.currentTimeMillis
    
    val appConfig = Config(date = date, s3Prefix = s3Prefix, rate = rate)
    val sparkConf = new SparkConf()
      .setAppName("UsersReadPerf")
    
    val sc = new SparkContext(sparkConf)
    setS3Credentials(sc)

    println(s"> Env [${Env.env}] Date [${appConfig.date}] Rate [${appConfig.rate}] Input [${appConfig.inputFolder}]")

    val total = sc.textFile(appConfig.inputFolder)
      .map { 
        line => {
          val track : Option[Track] = line.split("\\t")
          track foreach {
            obj => {
              UsersOps.read(obj).onComplete {
                case Success(user) => onSuccess(user)
                case Failure(e)    => onFailure(e) 
              }
            }
          }
        }
      }.count
    
    println(s" = total successful read $nrSuccess, failed read $nrFailure")
    
    sc.stop()
    // force exit otherwise hang    
    System.exit(0)
  }

  def onSuccess(user: Option[com.acme.cass.User]) = {
    Lock.synchronized {
      if (nrSuccess % 100000 == 0) {
        println(user)
      }
      
      nrSuccess += 1 
    }
  }

  def onFailure(err: Throwable) = {
    err.printStackTrace
    Lock.synchronized {
      nrFailure += 1
    }
  }

}