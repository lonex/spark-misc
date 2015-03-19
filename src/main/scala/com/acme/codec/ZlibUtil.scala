package com.acme.codec

import java.util.zip.{Inflater, Deflater}
import java.nio.file.{Files, Paths}
import java.io.{File, FileOutputStream}

object ZlibUtil {

  def compress(inData: Array[Byte]): Array[Byte] = {
    var deflater: Deflater = new Deflater()
    deflater.setInput(inData)
    deflater.finish
    val compressedData = new Array[Byte](inData.size)
    val count: Int = deflater.deflate(compressedData)
    return compressedData.take(count)
  }

  def decompress(inData: Array[Byte]): Array[Byte] = {
    val inflater = new Inflater()
    inflater.setInput(inData)
    val decompressedData = new Array[Byte](inData.size * 2)
    var count = inflater.inflate(decompressedData)
    var finalData = decompressedData.take(count)
    while (count > 0) {
      count = inflater.inflate(decompressedData)
      finalData = finalData ++ decompressedData.take(count)
    }
    return finalData
  }

}

