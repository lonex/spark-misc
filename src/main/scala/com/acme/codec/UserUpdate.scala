package com.acme.codec

import org.joda.time._
import java.util.zip.Inflater
import com.lambdaworks.jacks._

// zjson is the compressed version of the user data
case class UserUpdateFields(userIdJson: String, lastUpdateJson: String, zjson: String)

// data is a list of data providers for now
case class UserUpdate(userId: String, lastUpdate: Int, data: Iterable[String])

object UserUpdate {
  //
  // e.g.
  // zjson^C{"s":"emyiAAAAAAAAAHicqlaKL1GyqlYqAxGJ6UpWhiYGluamBpaW5rW1OmCRaqWiEqA0kF2WWZxZkl-kk5ienFhUoqQDksgphugEiyDp1kGoRze0pCgzPT0TbG1pcWpRfGYK0HRPU5egIN2kpOD0SpOIgrD8QkfX0MB0pdpaQAAAAP__upkykg=="}^Buser_id^C{"s":"I5DRR-bbSgy4XpVoqAEUQg"}^Blast_update^C{"n":"1409750997"}
  //
  val FIELD_SEP = "\\u0002" // field separator is Ctrl-B
  val VALUE_SEP = "\\u0003" // value separator is Ctrl-C
  val TYPE_SEP = ":" // data type separator is :

  val zjsonFieldName = "zjson"
  val userIdFieldName = "user_id"
  val lastUpdateFieldName = "last_update"

  // zlib compressed string for the user has "zl" (2 bytes) plus the length (8 Bytes) prepended as part
  // part of the final data blob. We discard these 10 bytes before decompression
  val zlibCompressStartAt = 10

  def splitNameValue(line: String): (String, String) = {
    val Array(attrName, attrValue) = line.split(VALUE_SEP)
    (attrName, attrValue)
  }

  def decodeLogLine(line: String): Option[UserUpdateFields] = {
    val attrs = line.split(FIELD_SEP)

    var userId = None: Option[String]
    var zjson = None: Option[String]
    var lastUpdate = None: Option[String]

    for (attr <- attrs) {
      splitNameValue(attr) match {
        case (field, v) => if (field == zjsonFieldName)
          zjson = Some(v)
        else if (field == userIdFieldName)
          userId = Some(v)
        else if (field == lastUpdateFieldName)
          lastUpdate = Some(v)
      }
    }

    if (userId.isDefined && lastUpdate.isDefined && zjson.isDefined)
      Some(UserUpdateFields(userId.get, lastUpdate.get, zjson.get))
    else
      None
  }

  val jsonTypeKey = "_t"
  val jsonTypeVKey = "v"
  val jsonTKey = "triggit"
  val jsonTUserIdKey = "user_id"

  val dynamoNumber = "n"
  val dynamoString = "s"

  //  {"0Xk"=>
  //  {"rtv"=>"xxx,yyyy",
  //   "rtvls"=>{"xxx"=>1404765875, "yyyy"=>1404765875},
  //   "rv"=>["0XkDE121C07I-K11"],
  //   "rvls"=>{"0XkDE121C07I-K11"=>1404765875}},
  // "_t"=>{"v"=>{"0Xk"=>1404765875}},
  // "ppkc"=>{"0Xk"=>{"0XkDE121C07I-K11"=>0}},
  // "TKey"=>{"user_id"=>"M-fEFEAeR6C-MlTp-EFnEA"}}
  //  
  def parseZJson(source: String): Option[Iterable[String]] = {
    val jsonObj = JacksMapper.readValue[Map[String, AnyRef]](source)
    try {
      val dataProviderIds = jsonObj(jsonTypeKey).asInstanceOf[Map[String, AnyRef]](jsonTypeVKey).asInstanceOf[Map[String, Int]].keys
      val userId = jsonObj(jsonTKey).asInstanceOf[Map[String, String]](jsonTUserIdKey).asInstanceOf[String]
      Some(dataProviderIds)
    } catch {
      // some of the user data doesn't have "_t" field at all
      case e: java.util.NoSuchElementException => None
    }
  }

  // @return Option[UserUpdate]
  // e.g.
  //   Some(UserUpdate(M-fEFEAeR6C-MlTp-EFnEA,1404765876, Set(0Xk)))
  def decode(line: String): Option[UserUpdate] = {
    val userLogLine = decodeLogLine(line)
    if (userLogLine.isDefined) {
      val ull = userLogLine.get
      val userId = ull.userIdJson
      val lastUpdate = ull.lastUpdateJson
      val zjson = ull.zjson

      val jStr = JacksMapper.readValue[Map[String, String]](zjson).get(dynamoString)
      if (jStr.isDefined) {
        val base64 = Base64Util.decodeBinary(jStr.get)
        val objStr = ZlibUtil.decompress(base64.slice(zlibCompressStartAt, base64.length)).map(_.toChar).mkString
        val userDataProviderMap = parseZJson(objStr)
        if (userDataProviderMap.isDefined) {
          val updateS = JacksMapper.readValue[Map[String, String]](lastUpdate).get(dynamoNumber)
          val updateTs = updateS map (_.toInt) get

          val uId = JacksMapper.readValue[Map[String, String]](userId).get(dynamoString).get
          return Some(UserUpdate(uId, updateTs, userDataProviderMap.get))
        }
      }
    }
    return None
  }
}

