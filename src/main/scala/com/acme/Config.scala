package com.acme

import collection.mutable.HashMap
import com.typesafe.config.{Config => TypeSafeConfig, _}

case class Config(
  env: String = Env.development,
  inFolder: String = ".", 
  outFolder: String = ".") 
{
  def this(m: Map[String, String]) = 
    this(m.getOrElse("env", Env.development), 
         m.getOrElse("inFolder", "."),
         m.getOrElse("outFolder", ".")
         )
}

object Config {
  //
  // Spark does not understand the application.conf in the root of the classpath of the Jar
  // We use the --driver-java-options "-Dapp.conf=path/to/application.conf" and 
  //                 --conf "spark.executor.extraJavaOptions=-Dapp.conf=path/to/application.conf"
  // at launch
  // to make the driver and the executor consider extra JVM options.
  //
  lazy val appConf : TypeSafeConfig = {
    sys.props.get("app.conf") match {
      case Some(s) => loadConfig(s)
      case None => ConfigFactory.load()
    }
  }
  
  def loadConfig(file: String) : TypeSafeConfig = {
     val fileConfig = ConfigFactory.parseFile(new java.io.File(file))
     ConfigFactory.load(fileConfig)
  }  
}

