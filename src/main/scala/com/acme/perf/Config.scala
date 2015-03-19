package com.acme.perf

import com.acme.Env
import com.acme.Util

case class Config(s3Prefix: String, date: String, rate: Double = 1.0) 
{
  val DefaultS3Prefix = "s3n://warehouse/tracks"
  val DefaultDate = "2014-06-11"
  
  def inputFolder = {
    if (!Util.isBlank(Option(s3Prefix))) {
      if (!Util.isBlank(Option(date)))
        s"${s3Prefix}/${date}/**/*"
      else
        s"${s3Prefix}/**/*"
    } else {
      if (Env.env == Env.production) { 
        if (!Util.isBlank(Option(date)))
          s"${DefaultS3Prefix}/${date}/**/*"
        else
          throw new IllegalArgumentException("Missing date")
      }
      else {
        s"sample/tracks/**/*"
      }
    }
  }

}
