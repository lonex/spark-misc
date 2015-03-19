package com.acme.cass

import com.acme.Config
import com.acme.Env

trait UUConnector extends DefaultConnector {

  def keyspaceConfig: String = {
    val keyspace = List("cassandra", com.acme.Env.env, "keyspace").mkString(".")
    Config.appConf.getString(keyspace)
  }

  val keySpace = keyspaceConfig
}