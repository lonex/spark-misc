package com.acme.cass

/*
 * Based on com.websudos.phantom.zookeeper.DefaultClusterStore
 */

import com.acme.Config
import java.net.InetSocketAddress

import scala.concurrent._
import scala.concurrent.duration._
import com.datastax.driver.core.{Cluster, Session}
import scala.util.{Success, Failure}

import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[cass] case object Lock

class EmptyClusterStoreException extends RuntimeException("Attempting to retrieve Cassandra cluster reference before initialisation")

class EmptyPortListException extends RuntimeException("Cannot build a cluster from an empty list of addresses")


object DefaultClusterStore {

  protected[this] var clusterStore: Cluster = null
  protected[this] var _session: Session = null
  private[this] val sessions = new scala.collection.mutable.LinkedHashMap[String, Session]() with scala.collection.mutable.SynchronizedMap[String, Session]

  private[this] var inited = false

  lazy val logger = LoggerFactory.getLogger("com.acme.cass")

  def isInited: Boolean = Lock.synchronized {
    inited
  }

  def setInited(value: Boolean) = Lock.synchronized {
    inited = value
  }

  protected[this] def keySpaceCql(keySpace: String): String = {
    s"CREATE KEYSPACE IF NOT EXISTS $keySpace WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};"
  }

  def initStore(keySpace: String)(implicit timeout: Duration): Unit = Lock.synchronized {
    if (!isInited) {
      createCluster()

      _session = blocking {
        val s = clusterStore.connect()
        s.execute(keySpaceCql(keySpace))
        s.execute(s"USE $keySpace;")
        s
      }
      sessions.put(keySpace, _session)
      setInited(value = true)
    }
  }

  @throws[EmptyPortListException]
  protected[this] def createCluster()(implicit timeout: Duration): Cluster = {
    val ports = hostnamePortPairs

    logger.info("> Configured hostname port pair " + ports.toString)
    
    if (ports.isEmpty) {
      throw new EmptyPortListException
    } else {
      clusterStore = Cluster.builder()
        .addContactPointsWithPorts(ports.asJava)
        .withoutJMXReporting()
        .withoutMetrics()
        .build()
      clusterStore
    }
  }

  @throws[EmptyClusterStoreException]
  def cluster()(implicit duration: Duration): Cluster = {
    if (isInited) {
      if (clusterStore.isClosed) {
        createCluster()
      } else {
        clusterStore
      }
    } else {
      throw new EmptyClusterStoreException
    }
  }

  @throws[EmptyClusterStoreException]
  def session: Session = {
    if (isInited) {
      _session
    } else {
      throw new EmptyClusterStoreException
    }
  }

  def parsePorts(data: String): Seq[InetSocketAddress] = {
    data.split("[\\s,]").map(_.split(":")).map {
      case Array(hostname, port) => new InetSocketAddress(hostname, port.toInt)
    }.toSeq
  }

  def hostnamePortPairs: Seq[InetSocketAddress] = Lock.synchronized {
    parsePorts(urisConfig)
  }

  def urisConfig : String = {
    val uris = List("cassandra", com.acme.Env.env, "uris").mkString(".")
    Config.appConf.getString(uris)
  }
  
}
