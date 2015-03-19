package com.acme.cass

import java.util.UUID
import org.joda.time.DateTime
import com.websudos.phantom.Implicits._
import com.websudos.phantom.iteratee.Iteratee
import scala.concurrent.{Future => ScalaFuture}
import com.datastax.driver.core.{ ResultSet, Row }

case class User(id: String, ts: DateTime, events: Map[String, String] = Map())

sealed class Users extends CassandraTable[Users, User] {
  object id extends StringColumn(this) with PartitionKey[String]
  object ts extends DateTimeColumn(this) with PrimaryKey[DateTime]
  object events extends MapColumn[Users, User, String, String](this)
  
  def fromRow(row: Row): User = {
    User(
      id(row),
      ts(row),
      events(row)
    )
  }
}

object Users extends Users with UUConnector {
  def insertNewRecord(user: User): ScalaFuture[ResultSet] = {
    insert.value(_.id, user.id)
      .value(_.ts, user.ts)
      .value(_.events, user.events)
      .future()
  }

  def getUserByIdAndTs(id: String, ts: DateTime): ScalaFuture[Option[User]] = {
    select.where(_.id eqs id).and(_.ts eqs ts).one()
  }
  
  def getEventsByIdAndTsRange(id: String, start: DateTime, end: DateTime) :  ScalaFuture[Seq[User]] = {
    select.where(_.id eqs id).and(_.ts lte end).and(_.ts gte start).fetch()
  }
}
