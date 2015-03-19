package com.acme.dynamo

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props

import awscala.dynamodbv2._

class UserDeleteActor(dynamoDB: DynamoDB) extends Actor {
  
  private def removeUser(user: User) = User.delete(user)(dynamoDB)
  
  def receive = {
    case user @ User(userId, lastUpdate) => {
      removeUser(user)
      sender ! 1
    }
    case _ => println("Invalid user received") 
  }
}