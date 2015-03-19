package com.acme.dynamo

class RateControl {

  // Make it 'magicRateFactor' times faster to maximize the read based on the Provisioned Read Capacity Unit 
  val magicRateFactor = 1
  val rnd = util.Random

  def execute(rate: Double, scanned: Long, used: Long, factor : Double = magicRateFactor) = {
    val targetSpentTime = ( scanned / rate / factor * 1000).toInt
    if (targetSpentTime - used > 0) {
      Thread.sleep(targetSpentTime - used + rnd.nextInt(100))
    }
  }

}