package com.acme.perf

import com.acme.cass._
import com.acme.Util
import com.acme.codec.warehouse.Track
import scala.concurrent.{ Future => ScalaFuture }
import com.datastax.driver.core.ResultSet

object UsersOps {

  
//COLUMN_NAME DATA_TYPE
//time_stamp  timestamp
//import_time_stamp timestamp
//time_stamp_unix integer
//user_id varchar(2000)
//segment varchar(2000)
//family  varchar(100)
//value varchar(2000)
//user_agent  varchar(65535)
//ip  varchar(256)
//version integer
//hostname  varchar(256)
//

  val FamilyId = "_p"
  val Segment = "_s"
  
  def toUser(track: Track) : User = {
    val evts = collection.mutable.HashMap[String, String]()
    if (! Util.isBlank(Option(track.segment))) {
      evts(Segment) = track.segment
    }
    if (! Util.isBlank(Option(track.family))) {
      evts(FamilyId) = track.family
    }

    User(id = track.userId, ts = track.timeStamp, events = evts.toMap)
  }
  
  def read(track: Track) : ScalaFuture[Option[User]] = {
    val user = toUser(track)
    Users.getUserByIdAndTs(user.id, user.ts)
  }
  
  def upsert(track: Track) : ScalaFuture[ResultSet] = {
    val user = toUser(track)
    Users.insertNewRecord(user)
  }
}