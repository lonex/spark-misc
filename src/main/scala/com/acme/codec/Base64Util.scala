package com.acme.codec

import org.apache.commons.codec.binary.{ Base64 => B64 }

object Base64Util {
  final private val b64 = new B64
  final val utf8 = "UTF-8"
  
  def encodeString(in: String): String = encodeString(in.getBytes(utf8))
  
  def encodeString(in: Array[Byte]): String = new String(b64.encode(in))
  
  def encodeBinary(in: String): Array[Byte] = b64.encode(in.getBytes(utf8))
  
  def encodeBinary(in: Array[Byte]): Array[Byte] = b64.encode(in)
  
  def decodeString(in: Array[Byte]): String = new String(decodeBinary(in))
  
  def decodeString(in: String): String = decodeString(in.getBytes(utf8))
  
  def decodeBinary(in: String): Array[Byte] = decodeBinary(in.getBytes(utf8))
  
  def decodeBinary(in: Array[Byte]): Array[Byte] = (new B64).decode(in)
}
