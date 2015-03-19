package com.acme.cass

import org.specs2.mutable._
import org.specs2.specification.Scope
import org.joda.time.DateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import scala.concurrent.Await
import scala.concurrent.duration._

class UserSpec extends Specification {

  "Read write to Cassandra" should {
    val testUser = User( "random-user-id", DateTime.now, Map("a" -> "some-auction", "b" -> "some-buy"))
    val oneSecond = Duration(1000, "millis")
  
    "Upsert and Retrieval" in {
      val result = Await.result(Users.insertNewRecord(testUser), oneSecond)
      val user = Await.result(Users.getUserByIdAndTs(testUser.id, testUser.ts), oneSecond).get
      user.id mustEqual testUser.id
      user.ts mustEqual testUser.ts
      user.events mustEqual testUser.events
    }
  }
}