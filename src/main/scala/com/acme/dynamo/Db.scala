package com.acme.dynamo

import awscala._
import awscala.dynamodbv2._

class Db(val config: Config) {
  def client = if (config.env == com.acme.Env.production) {
    config.region match {
      case Db.US_EAST_1 => DynamoDB.at(Region.US_EAST_1)
      case Db.US_WEST_1 => DynamoDB.at(Region.US_WEST_1)
      case _            => DynamoDB.at(Region.US_WEST_2)
    }
  } else
    DynamoDB.at(Region.US_WEST_2)
}

object Db {
  val US_WEST_1 = "US_WEST_1"
  val US_WEST_2 = "US_WEST_2"
  val US_EAST_1 = "US_EAST_1"
  val Regions = List(US_EAST_1, US_WEST_1, US_WEST_2)
}