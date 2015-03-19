package com.acme.codec.warehouse

import org.joda.time.DateTime
import com.acme.Util

/**
 * Mapping for Segment s3 data
 */

//
// 2014:06:10 23:48:01     2014:06:11 00:00:00     1402444081      pRa413laT-KrMqQ9gX3lsA  
// 131201402861                    
// Mozilla/5.0 (iPod touch; CPU iPhone OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53  
// 67.2.222.100    1       sample-event
//
//COLUMN_NAME DATA_TYPE
//time_stamp  timestamp
//import_time_stamp timestamp
//time_stamp_unix integer
//user_id varchar(2000)
//segment varchar(2000)
//family varchar(256)
//value varchar(2000)
//user_agent  varchar(65535)
//ip  varchar(256)
//version integer
//hostname  varchar(256)
//

case class Track(
  timeStamp: DateTime,
  importTimeStamp: DateTime,
  timeStampUnix: Int,
  userId: String,
  segment: String,
  family: String,
  value: String,
  userAgent: String,
  ip: String,
  version: Int,
  hostname: String
)

object Track {
  implicit def toTrack(arr: Array[String]): Option[Track] = {
    try {
      arr match {
        case Array(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10) =>
          Some( (Track.apply _)
            .tupled((Util.stringToDateTime(a1), Util.stringToDateTime(a2), a3.toInt, a4, a5, a6, a7, a8, a9, a10.toInt, ""))
          )
        case Array(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, _*) =>
          Some( (Track.apply _)
            .tupled((Util.stringToDateTime(a1), Util.stringToDateTime(a2), a3.toInt, a4, a5, a6, a7, a8, a9, a10.toInt, a11)
          ))
        case _ => None
      }
    } catch {
      case x: Throwable => {println(x); None}
    }
  }
}
