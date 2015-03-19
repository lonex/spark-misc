package com.acme.cass

import java.net.InetSocketAddress
import org.slf4j.{Logger, LoggerFactory}
import com.datastax.driver.core.{Cluster, Session}
import scala.concurrent._
import scala.concurrent.duration._

/*
 * Based on com.websudos.phantom.zookeeper.ZookeeperManager
 */

trait CassandraManager {

  def cluster: Cluster
  def initIfNotInited(keySpace: String)

  implicit def session: Session
}

class DefaultConnectionManager extends CassandraManager {

  implicit val timeout: Duration = 2.seconds

  lazy val logger = LoggerFactory.getLogger("com.acme.cass")

  val store = DefaultClusterStore

  def cluster: Cluster = store.cluster

  def session: Session = store.session

  def initIfNotInited(keySpace: String) = store.initStore(keySpace)
}

object DefaultManager {
  lazy val defaultManager = new DefaultConnectionManager
}

trait DefaultConnector {

  def keySpace: String

  val manager = DefaultManager.defaultManager

  implicit lazy val session: Session = {
    manager.initIfNotInited(keySpace)
    manager.session
  }
}

