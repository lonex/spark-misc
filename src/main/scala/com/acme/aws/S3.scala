package com.acme.aws

import com.acme.Config
import com.acme.Env

trait S3 {
  val S3Key : String = Config.appConf.getString("s3.AWS_ACCESS_KEY_ID")
  
  val S3Secret : String = Config.appConf.getString("s3.AWS_SECRET_ACCESS_KEY")
}