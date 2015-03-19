package com.acme

object Env {
  val development = "development"
  val production = "production"
  val test = "test"
  val envs = Seq(development, production, test)
  val envString = "env"
  
  def env : String = {
    sys.props.get(envString) match {
      case Some(s) => s
      case None => development
    }
  }
}
