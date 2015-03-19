package com.acme.dynamo

import com.acme.Env
import awscala._

case class Config(env: String = Env.development, region: String = Db.US_WEST_2) 
{
  def this(m: Map[String, String]) = this(m.getOrElse("env", Env.development),
                                          m.getOrElse("region", Db.US_WEST_2))
}
