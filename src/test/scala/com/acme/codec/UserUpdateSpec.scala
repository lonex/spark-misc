package com.acme.codec

import org.specs2.mutable._
import org.specs2.specification.Scope

class UserUpdateSpec extends Specification {
  
  def logLine = """zjson\u0003""" + 
                """{"s":"emzDAAAAAAAAAHicqlYySzJIMUtKMUuzNE8xszA0V7KqVioqKVOyUgpKLShNyslMTswLSCwqqQxILSouTSzOzM9T0gGpyCkGKcWtyMrQxNDU0tLI1MiotlZHKb4EpLwMRGBaiaq0pCgzPT0TrL60OLUoPjMF6JrQ-ER3U3PH4KBK_8Sq5OyCrCwvY_d0pdpaQAAAAP__YH4_Bg=="}""" +
                """\u0002user_id\u0003{"s":"U_aG57ASRyOazckpjjJ3Gg"}\u0002last_update\u0003{"n":"1415992523"}"""
  
  def incompleteLogLine = """zjson\u0003""" + 
                          """{"s":"emwwAAAAAAAAAHicqlYqKcpMT88sUbKqViotTi2Kz0xRslIyrrTw9spPLQ2M987x8c_MTixwyvUtV6qtBQQAAP__nZEQ8g=="}""" + 
                          """\u0002user_id\u0003{"s":"3y8KJoeuQ_KlLOikapBmMw"}\u0002last_update\u0003{"n":"1419331180"}"""
                          
  def expectedUU = UserUpdate("U_aG57ASRyOazckpjjJ3Gg", 1415992523, Set("6b0d6bd6f97d6817"))
 
                
  "The log line parsing" should {
    "be separated correctly" in {
      val uuFields = UserUpdate.decodeLogLine(logLine)
      "The result UserUpdateFields must be defined" ! (uuFields.isDefined == true)
    }
  }

  isolated
  
  "The fields decoding" should {
	val uu = UserUpdate.decode(logLine).get
    val incompleteUU = UserUpdate.decode(incompleteLogLine)

    "user_id field" in {
      uu.userId mustEqual expectedUU.userId
    }
    
    "lastUpdate field" in {
      uu.lastUpdate mustEqual expectedUU.lastUpdate
    }
    
    "data field" in {
      uu.data mustEqual expectedUU.data
    }

    "if zjson only has the 'user_id' but misses other fields e.g. '_t'" in {
      incompleteUU must beNone
    }
    
  }
  
}

