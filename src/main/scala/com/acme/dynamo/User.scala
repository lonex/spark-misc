package com.acme.dynamo

import awscala.dynamodbv2._
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException
import com.acme.Env

case class User(user_id: String, last_update: Int)

object User {
  val TABLE = "users"
  val LAST_UPDATE = "last_update"
  val USER_ID = "user_id"

  val WRITE_CAP = 100 // Default write capacity in Unit
  val READ_CAP = 1000 // Default read capacity in Unit
  
  val invalidUserId = ""
  val invalidLastUpdate = "-1"

  def itemCount(env: String) =
    if (env == Env.production)
      8*1000*1000*1000L
    else
      1000000
      
  def tableMeta = Table(
    name = TABLE,
    hashPK = USER_ID,
    attributes = Seq(AttributeDefinition(USER_ID, AttributeType.String)),
    provisionedThroughput = Option(ProvisionedThroughput(readCapacityUnits = READ_CAP, writeCapacityUnits = WRITE_CAP))
  )

  def createTable(implicit dynamoDB: DynamoDB): Table = {
    try {
      val meta = dynamoDB.createTable(tableMeta)
    } catch {
      case e: ResourceInUseException => println(s"Warn: Cannot create preexisting table $TABLE")
    }
    dynamoDB.table(TABLE).get
  }
  
  def tableRef(implicit dynamoDB: DynamoDB): Option[Table] = {
    dynamoDB.table(TABLE)
  }

  def insert(table: Table, record: User)(implicit dynamoDB: DynamoDB) = table.put(record.user_id, LAST_UPDATE -> record.last_update)

  def delete(user: User)(implicit dynamoDB: DynamoDB) = {
    tableRef.foreach {
      table =>
        try {
          print(".")
          table.delete(hashPK = user.user_id)
        } catch {
          case e : com.amazonaws.AmazonServiceException => println(s"Error: $e") 
          case e : Throwable => println("Error: $e")
        }
    }
  }
  
  def decode(attrs: Seq[Attribute]) : Option[User] = {
    val userId = attrs.find(_.name == USER_ID) match {
      case Some(Attribute(name, value)) => value.s getOrElse invalidUserId
      case _ => invalidUserId
    }

    val lastUpdate = attrs.find(_.name == LAST_UPDATE) match {
      case Some(Attribute(name, value)) => value.n getOrElse invalidLastUpdate
      case _ => invalidLastUpdate
    }
    
    if (userId == invalidUserId || lastUpdate == invalidLastUpdate)
      None
    else 
      Some(User(userId, lastUpdate.toInt))
  }

}