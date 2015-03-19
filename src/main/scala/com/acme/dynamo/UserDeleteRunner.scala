package com.acme.dynamo

import com.acme.Util
import collection.mutable.ArrayBuffer

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

import awscala.dynamodbv2._

class UserDeleteRunner(users: ArrayBuffer[User], segment: Int, dynamoDB: DynamoDB) extends Actor {
  var nrUsers = users.size
  val totalUsers = nrUsers
  val deleteActor = context.actorOf(Props(new UserDeleteActor(dynamoDB)), "user-delete-actor")
  val start = System.currentTimeMillis

  for (user <- users) {
    deleteActor ! user
  }
  
  def receive = {
    case nr : Int => 
      nrUsers -= 1
      if (nrUsers == 0) {
        println(s"[${Util.utcNow}] Tried to delete $totalUsers in segment $segment, took ${System.currentTimeMillis - start} ms")
        context.system.shutdown
      }
  }

}
