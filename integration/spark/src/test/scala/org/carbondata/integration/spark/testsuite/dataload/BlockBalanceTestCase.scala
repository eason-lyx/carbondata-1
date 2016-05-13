package org.carbondata.integration.spark.testsuite.dataload

import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.load.BlockDetails
import org.carbondata.integration.spark.rdd.CarbonDataRDDFactory
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable.ArrayBuffer


class BlockBalanceTestCase extends QueryTest with BeforeAndAfterAll {

  test("Block balance test case with all local block") {
    val blockList: ArrayBuffer[BlockDetails] = new ArrayBuffer[BlockDetails]()
    val hostname: Array[String] = Array("1", "2", "3")
    val blockNumInHost: Array[Int] = Array(5, 4, 5)
    val blockDetails1 = new BlockDetails
    blockDetails1.setLocations(Array("1"))
    blockList.+=(blockDetails1)
    val blockDetails2 = new BlockDetails
    blockDetails2.setLocations(Array("3"))
    blockList.+=(blockDetails2)
    val blockDetails3 = new BlockDetails
    blockDetails3.setLocations(Array("1", "2"))
    blockList.+=(blockDetails3)
    val blockDetails4 = new BlockDetails
    blockDetails4.setLocations(Array("2", "3"))
    blockList.+=(blockDetails4)
    val blockDetails5 = new BlockDetails
    blockDetails5.setLocations(Array("1", "3"))
    blockList.+=(blockDetails5)
    val blockDetails6 = new BlockDetails
    blockDetails6.setLocations(Array("1", "2", "3"))
    blockList.+=(blockDetails6)
    val blockDetails7 = new BlockDetails
    blockDetails7.setLocations(Array("1", "2", "3"))
    blockList.+=(blockDetails7)
    val blockGroupBy = CarbonDataRDDFactory.assignBlocksToHost(hostname, blockNumInHost, blockList)
    var blocksNum = 0
    blockGroupBy.foreach { groupby =>
      blocksNum += groupby._2.length
    }
    assert(blocksNum == 7)
    assert(blockGroupBy.toMap.get("1").get.length == 3)
  }

  test("Block balance test case with no local block") {
    val blockList: ArrayBuffer[BlockDetails] = new ArrayBuffer[BlockDetails]()
    val hostname: Array[String] = Array("1", "2", "3")
    val blockNumInHost: Array[Int] = Array(3, 0, 1)
    val blockDetails10 = new BlockDetails
    blockDetails10.setLocations(Array("1"))
    blockList.+=(blockDetails10)
    val blockDetails20 = new BlockDetails
    blockDetails20.setLocations(Array("3"))
    blockList.+=(blockDetails20)
    val blockDetails30 = new BlockDetails
    blockDetails30.setLocations(Array("1"))
    blockList.+=(blockDetails30)
    val blockDetails40 = new BlockDetails
    blockDetails40.setLocations(Array("1"))
    blockList.+=(blockDetails40)
    var blocksNum = 0
    val blockGroupBy = CarbonDataRDDFactory.assignBlocksToHost(hostname, blockNumInHost,
      blockList)
    blockGroupBy.foreach { groupby =>
      blocksNum += groupby._2.length
    }
    // total block num is 4
    assert(blocksNum == 4)
    // location 1 get 2 block
    assert(blockGroupBy.toMap.get("1").get.length == 2)
    // location 3 get the local block
    assert(blockGroupBy.toMap.get("3").get.apply(0).eq(blockDetails20))
    // the no local block from location 1
    assert(blockGroupBy.toMap.get("2").get.apply(0).getLocations.contains("1"))
  }

}
