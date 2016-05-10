/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.carbondata.integration.spark.rdd

import scala.StringBuilder
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.{DummyLoadRDD, NewHadoopRDD}
import org.apache.spark.sql.{CarbonEnv, CarbonRelation, SQLContext}
import org.apache.spark.sql.cubemodel.Partitioner
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.util.{FileUtils, SplitUtils}

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{CarbonDataLoadSchema, CarbonDef}
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.carbondata.core.metadata.CarbonMetadata
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.integration.spark._
import org.carbondata.integration.spark.load._
import org.carbondata.integration.spark.merger.CarbonDataMergerUtil
import org.carbondata.integration.spark.splits.TableSplit
import org.carbondata.integration.spark.util.{CarbonQueryUtil, LoadMetadataUtil}
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory extends Logging {

  val logger = LogServiceFactory.getLogService(CarbonDataRDDFactory.getClass().getName());

  def intializeCarbon(sc: SparkContext, schema: CarbonDef.Schema, dataPath: String,
                      cubeName: String, schemaName: String, partitioner: Partitioner,
                      columinar: Boolean) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl();
    val catalog = CarbonEnv.getInstance(sc.asInstanceOf[SQLContext]).carbonCatalog
    val cubeCreationTime = catalog.getCubeCreationTime(schemaName, cubeName)
    new CarbonDataCacheRDD(sc, kv, schema, catalog.storePath, cubeName, schemaName, partitioner,
      columinar, cubeCreationTime).collect
  }

  // scalastyle:off
  def partitionCarbonData(sc: SparkContext,
                          schemaName: String,
                          cubeName: String,
                          sourcePath: String,
                          targetFolder: String,
                          requiredColumns: Array[String],
                          headers: String,
                          delimiter: String,
                          quoteChar: String,
                          escapeChar: String,
                          multiLine: Boolean,
                          partitioner: Partitioner): String = {
    // scalastyle:on
    val status = new
        CarbonDataPartitionRDD(sc, new PartitionResultImpl(), schemaName, cubeName, sourcePath,
          targetFolder, requiredColumns, headers, delimiter, quoteChar, escapeChar, multiLine,
          partitioner).collect
    CarbonDataProcessorUtil
      .renameBadRecordsFromInProgressToNormal("partition/" + schemaName + '/' + cubeName);
    var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    status.foreach {
      case (key, value) =>
        if (value == true) {
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
        }
    }
    loadStatus
  }

  def mergeCarbonData(
                       sc: SQLContext,
                       carbonLoadModel: CarbonLoadModel,
                       storeLocation: String,
                       hdfsStoreLocation: String,
                       partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl();
    val cube = CarbonMetadata.getInstance()
      .getCubeWithCubeName(carbonLoadModel.getTableName(), carbonLoadModel.getDatabaseName());
    val metaDataPath: String = cube.getMetaDataFilepath()
    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
  }

  def deleteLoadByDate(
                        sqlContext: SQLContext,
                        schema: CarbonDataLoadSchema,
                        schemaName: String,
                        cubeName: String,
                        tableName: String,
                        hdfsStoreLocation: String,
                        dateField: String,
                        dateFieldActualName: String,
                        dateValue: String,
                        partitioner: Partitioner) {

    val sc = sqlContext;
    // Delete the records based on data
    var cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + cubeName)

    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    val loadMetadataDetailsArray = CarbonUtil.readLoadMetadata(cube.getMetaDataFilepath()).toList
    val resultMap = new CarbonDeleteLoadByDateRDD(
      sc.sparkContext,
      new DeletedLoadResultImpl(),
      schemaName,
      cube.getDatabaseName,
      dateField,
      dateFieldActualName,
      dateValue,
      partitioner,
      cube.getFactTableName,
      tableName,
      hdfsStoreLocation,
      loadMetadataDetailsArray,
      currentRestructNumber).collect.groupBy(_._1).toMap

    var updatedLoadMetadataDetailsList = new ListBuffer[LoadMetadataDetails]()
    if (!resultMap.isEmpty) {
      if (resultMap.size == 1) {
        if (resultMap.contains("")) {
          logError("Delete by Date request is failed")
          sys.error("Delete by Date request is failed, potential causes " +
            "Empty store or Invalid column type, For more details please refer logs.")
        }
      }
      val updatedloadMetadataDetails = loadMetadataDetailsArray.map { elem => {
        var statusList = resultMap.get(elem.getLoadName())
        // check for the merged load folder.
        if (statusList == None && null != elem.getMergedLoadName()) {
          statusList = resultMap.get(elem.getMergedLoadName())
        }

        if (statusList != None) {
          elem.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime())
          // if atleast on CarbonCommonConstants.MARKED_FOR_UPDATE status exist,
          // use MARKED_FOR_UPDATE
          if (statusList.get
            .forall(status => status._2 == CarbonCommonConstants.MARKED_FOR_DELETE)) {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
          } else {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE)
            updatedLoadMetadataDetailsList += elem
          }
          elem
        } else {
          elem
        }
      }

      }

      // Save the load metadata
      var carbonLock = CarbonLockFactory
        .getCarbonLockObj(cube.getMetaDataFilepath(), LockUsage.METADATA_LOCK)
      try {
        if (carbonLock.lockWithRetries()) {
          logInfo("Successfully got the cube metadata file lock")
          if (!updatedLoadMetadataDetailsList.isEmpty) {
            LoadAggregateTabAfterRetention(schemaName, cube.getFactTableName, cube.getFactTableName,
              sqlContext, schema, updatedLoadMetadataDetailsList)
          }

          // write
          CarbonLoaderUtil.writeLoadMetadata(
            schema,
            schemaName,
            cube.getDatabaseName,
            updatedloadMetadataDetails.asJava)
        }
      } finally {
        if (carbonLock.unlock()) {
          logInfo("unlock the cube metadata file successfully")
        } else {
          logError("Unable to unlock the metadata lock")
        }
      }
    } else {
      logError("Delete by Date request is failed")
      logger.audit("The delete load by date is failed.");
      sys.error("Delete by Date request is failed, potential causes " +
        "Empty store or Invalid column type, For more details please refer logs.")
    }
  }

  def LoadAggregateTabAfterRetention(
                                      schemaName: String,
                                      cubeName: String,
                                      factTableName: String,
                                      sqlContext: SQLContext,
                                      schema: CarbonDataLoadSchema,
                                      list: ListBuffer[LoadMetadataDetails]) {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) sys.error(s"Cube $schemaName.$cubeName does not exist")
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(cubeName)
    carbonLoadModel.setDatabaseName(schemaName)
    val table = relation.cubeMeta.carbonTable
    val aggTables = schema.getCarbonTable.getAggregateTablesName
    if (null != aggTables && !aggTables.isEmpty) {
      carbonLoadModel.setRetentionRequest(true)
      carbonLoadModel.setLoadMetadataDetails(list.asJava)
      carbonLoadModel.setTableName(table.getFactTableName)
      carbonLoadModel
        .setCarbonDataLoadSchema(new CarbonDataLoadSchema(relation.cubeMeta.carbonTable));
      // TODO: need to fill dimension relation from data load sql command
      var storeLocation = CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
          System.getProperty("java.io.tmpdir"))
      storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
      if (null == kettleHomePath) {
        kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home");
      }
      if (kettleHomePath == null) sys.error(s"carbon.kettle.home is not set")
      CarbonDataRDDFactory.loadCarbonData(
        sqlContext,
        carbonLoadModel,
        storeLocation,
        relation.cubeMeta.dataPath,
        kettleHomePath,
        relation.cubeMeta.partitioner, columinar, true)
    }
  }

  def loadCarbonData(sc: SQLContext,
                     carbonLoadModel: CarbonLoadModel,
                     storeLocation: String,
                     hdfsStoreLocation: String,
                     kettleHomePath: String,
                     partitioner: Partitioner,
                     columinar: Boolean,
                     isAgg: Boolean,
                     partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS) {
    val cube = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var currentRestructNumber = -1
    try {
      logger.audit("The data load request has been received.");

      currentRestructNumber = CarbonUtil
        .checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath(), "RS_", false)
      if (-1 == currentRestructNumber) {
        currentRestructNumber = 0
      }

      // Check if any load need to be deleted before loading new data
//      deleteLoadsAndUpdateMetadata(carbonLoadModel, cube, partitioner, hdfsStoreLocation, false,
//        currentRestructNumber)
      if (null == carbonLoadModel.getLoadMetadataDetails) {
        readLoadMetadataDetails(carbonLoadModel, hdfsStoreLocation)
      }

      var currentLoadCount = -1
      if (carbonLoadModel.getLoadMetadataDetails().size() > 0) {
        for (eachLoadMetaData <- carbonLoadModel.getLoadMetadataDetails().asScala) {
          val loadCount = Integer.parseInt(eachLoadMetaData.getLoadName())
          if (currentLoadCount < loadCount) {
            currentLoadCount = loadCount
          }
        }
        currentLoadCount += 1;
        CarbonLoaderUtil
          .deletePartialLoadDataIfExist(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getTableName, hdfsStoreLocation,
            currentRestructNumber, currentLoadCount)
      } else {
        currentLoadCount += 1;
      }

      // reading the start time of data load.
      val loadStartTime = CarbonLoaderUtil.readCurrentTime();
      carbonLoadModel.setFactTimeStamp(loadStartTime);
      val cubeCreationTime = CarbonEnv.getInstance(sc).carbonCatalog
        .getCubeCreationTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
      val schemaLastUpdatedTime = CarbonEnv.getInstance(sc).carbonCatalog
        .getSchemaLastUpdatedTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)

      // get partition way from configuration
      val isTableSplitPartition = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.TABLE_SPLIT_PARTITION,
        CarbonCommonConstants.TABLE_SPLIT_PARTITION_DEFAULT_VALUE).toBoolean
      var blocksGroupBy: Array[(String, Array[BlockDetails])] = null
      isTableSplitPartition match {
        case true =>
          /*
           * when data handle by table split partition
           * 1) get partition files, direct load or not will get the different files path
           * 2) get files blocks by using SplitUtils
           * 3) output Array[(partitionID,Array[BlockDetails])] to blocksGroupBy
           */
          var splits = Array[TableSplit]()
          if (carbonLoadModel.isDirectLoad()) {
            // get all table Splits, this part means files were divide to different partitions
            splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath(),
              partitioner.nodeList, partitioner.partitionCount)
            // get all partition blocks from file list
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                for (path <- split.getPartition.getFilesPath.asScala) {
                  pathBuilder.append(path).append(",")
                }
                if (pathBuilder.size > 0) {
                  pathBuilder.substring(0, pathBuilder.size - 1)
                }
                (split.getPartition.getUniqueID, SplitUtils.getSplits(pathBuilder.toString(),
                  sc.sparkContext))
            }
          } else {
            // get all table Splits,when come to this, means data have been partition
            splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName, null, partitioner)
            // get all partition blocks from factFilePath/uniqueID/
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                pathBuilder.append(carbonLoadModel.getFactFilePath)
                if (!carbonLoadModel.getFactFilePath.endsWith("/")
                  && !carbonLoadModel.getFactFilePath.endsWith("\\")) {
                  pathBuilder.append("/")
                }
                pathBuilder.append(split.getPartition.getUniqueID).append("/")
                (split.getPartition.getUniqueID,
                  SplitUtils.getSplits(pathBuilder.toString, sc.sparkContext))
            }
          }

        case false =>
          /*
           * when data load handle by node partition
           * 1) clone the hadoop configuration,and set the file path to the configuration
           * 2) use DummyLoadRDD to get the task hosts
           * 3) filter all block location which no exist in task host
           * 4) sort block list by locations number,empty location block append to the end of list
           * 5) divide blocks to hosts
           */
          val hadoopConfiguration = new Configuration(sc.sparkContext.hadoopConfiguration)
          // FileUtils will skip file which is no csv, and return all file path which split by ','
          val filePaths = FileUtils.getPaths(carbonLoadModel.getFactFilePath)
          hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", filePaths)
          hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
          val newHadoopRDD = new NewHadoopRDD[LongWritable, Text](
            sc.sparkContext,
            classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
            classOf[LongWritable],
            classOf[Text],
            hadoopConfiguration)
          val hosts = new DummyLoadRDD(newHadoopRDD).distinct().collect().sorted
          val blockNumInHost: Array[Int] = new Array[Int](hosts.length)
          val blockListWithNoSorted = SplitUtils.getSplits(filePaths, sc.sparkContext)
            .map { block =>
              val locations = block.getLocations.toBuffer
              // filter location which no exist in hosts
              locations.foreach { location =>
                if (!hosts.contains(location)) {
                  locations.-=(location)
                } else {
                  blockNumInHost(hosts.indexOf(location)) += 1
                }
              }
              block.setLocations(locations.toArray)
              block
            }
          val blockLists: ArrayBuffer[BlockDetails] = new ArrayBuffer[BlockDetails]()
          val blockListsWithNonLocality: ArrayBuffer[BlockDetails]
          = new ArrayBuffer[BlockDetails]()
          // sort block by location number, if the locations is empty, then append to end of list
          blockListWithNoSorted.map { block =>
            if (block.getLocations.isEmpty) {
              blockListsWithNonLocality.append(block)
            }
            (block.getLocations.length, block)
          }.filter(_._1 != 0).sorted.map { block =>
            blockLists.+=(block._2)
          }
          blockLists.appendAll(blockListsWithNonLocality)
          blocksGroupBy = divideBlocksToHost(hosts, blockNumInHost, blockLists)
      }

      val status = new
          CarbonDataLoadRDD(sc.sparkContext, new ResultImpl(), carbonLoadModel, storeLocation,
            hdfsStoreLocation, kettleHomePath, partitioner, columinar, currentRestructNumber,
            currentLoadCount, cubeCreationTime, schemaLastUpdatedTime, blocksGroupBy,
            isTableSplitPartition).collect()
      val newStatusMap = scala.collection.mutable.Map.empty[String, String]
      status.foreach { eachLoadStatus =>
        val state = newStatusMap.get(eachLoadStatus._2.getPartitionCount)
        if (null == state || None == state ||
          state == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        } else if (state == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
          eachLoadStatus._2.getLoadStatus == CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS) {
          newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        }
      }

      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      newStatusMap.foreach {
        case (key, value) =>
          if (value == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } else if (value == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
            !loadStatus.equals(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
          }
      }

      if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
        partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
        loadStatus = partitionStatus
      }

      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        var message: String = ""
        logInfo("********starting clean up**********")
        if (isAgg) {
          CarbonLoaderUtil.deleteTable(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getAggTableName, hdfsStoreLocation,
            currentRestructNumber)
          message = "Aggregate table creation failure"
        } else {
          val (result, metadataDetails) = status(0)
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + result
          CarbonLoaderUtil.deleteSlice(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getTableName, hdfsStoreLocation,
            currentRestructNumber, newSlice)
          val schema = carbonLoadModel.getSchema
          val aggTables = schema.cubes(0).fact.asInstanceOf[CarbonDef.Table].aggTables
          if (null != aggTables && !aggTables.isEmpty) {
            aggTables.foreach { aggTable =>
              val aggTableName = CarbonLoaderUtil.getAggregateTableName(aggTable)
              CarbonLoaderUtil
                .deleteSlice(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
                  carbonLoadModel.getTableName, aggTableName, hdfsStoreLocation,
                  currentRestructNumber, newSlice)
            }
          }
          message = "Dataload failure"
        }
        logInfo("********clean up done**********")
        logger.audit("The data loading is failed.")
        logWarning("Unable to write load metadata file")
        throw new Exception(message)
      } else {
        val (result, metadataDetails) = status(0)
        if (!isAgg) {
          CarbonLoaderUtil
            .recordLoadMetadata(result, metadataDetails, carbonLoadModel, loadStatus, loadStartTime)
        } else if (!carbonLoadModel.isRetentionRequest()) {
          try {
            CarbonEnv.getInstance(sc).carbonCatalog.updateCube(carbonLoadModel.getSchema, false)(sc)
          } catch {
            case e: Exception =>
              CarbonLoaderUtil
                .deleteTable(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
                  carbonLoadModel.getTableName, carbonLoadModel.getAggTableName, hdfsStoreLocation,
                  currentRestructNumber)
              val message = "Aggregation creation failure"
              throw new Exception(message)
          }

          logInfo("********schema updated**********")
        }
        logger.audit("The data loading is successful.")
        if (CarbonDataMergerUtil
          .checkIfLoadMergingRequired(cube.getMetaDataFilepath(), carbonLoadModel,
            hdfsStoreLocation, partitioner.partitionCount, currentRestructNumber)) {

          val loadsToMerge = CarbonDataMergerUtil.getLoadsToMergeFromHDFS(
            hdfsStoreLocation, FileFactory.getFileType(hdfsStoreLocation),
            cube.getMetaDataFilepath(), carbonLoadModel, currentRestructNumber,
            partitioner.partitionCount);

          if (loadsToMerge.size() == 2) {
            val MergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
            var finalMergeStatus = true
            val mergeStatus = new CarbonMergerRDD(
              sc.sparkContext,
              new MergeResultImpl(),
              carbonLoadModel,
              storeLocation,
              hdfsStoreLocation,
              partitioner,
              currentRestructNumber,
              cube.getMetaDataFilepath(),
              loadsToMerge,
              MergedLoadName,
              kettleHomePath,
              cubeCreationTime).collect

            mergeStatus.foreach { eachMergeStatus =>
              val state = eachMergeStatus._2
              if (!state) {
                finalMergeStatus = false
              }
            }
            if (finalMergeStatus) {
              CarbonDataMergerUtil
                .updateLoadMetadataWithMergeStatus(loadsToMerge, cube.getMetaDataFilepath(),
                  MergedLoadName, carbonLoadModel)
            }
          }
        }
      }
    }

  }

  private def divideBlocksToHost(hosts: Array[String], blockNumInHost: Array[Int],
                                 blocksList: ArrayBuffer[BlockDetails])
  : Array[(String, Array[BlockDetails])] = {
    val blocksGroupby: ArrayBuffer[(String, BlockDetails)] =
      new ArrayBuffer[(String, BlockDetails)]()
    val totalBlockNum = blocksList.length
    val minBlocksNumPerHost = totalBlockNum / hosts.length
    val hostHandled: ArrayBuffer[String] = new ArrayBuffer[String]()
    var divideBlockOnebyOne = false
    val noLocalBlocksNumInHost: Array[Int] = new Array[Int](hosts.length)
    val remainBlocksList: ArrayBuffer[BlockDetails] = new ArrayBuffer[BlockDetails]()

    // loop until all blocks were divided
    while (blocksGroupby.length != totalBlockNum) {
      // all block locality were divided, now start divide the no local block
      if ((totalBlockNum - blocksGroupby.length) <= noLocalBlocksNumInHost.sum) {
        //get remain blocks to divide to hosts
        blocksList.foreach { block =>
          if (!blocksGroupby.map { groupByBlock =>
            groupByBlock._2
          }.contains(block)) {
            remainBlocksList.append(block)
          }
        }
        if (noLocalBlocksNumInHost.sum != 0) {
          for (i <- noLocalBlocksNumInHost.indices) {
            if (noLocalBlocksNumInHost(i) != 0) {
              for (j <- 0 until noLocalBlocksNumInHost(i)) {
                //alway choose the first, after divided, delete the first one.
                blocksGroupby.append((hosts(i), remainBlocksList(0)))
                remainBlocksList.remove(0)
              }
              noLocalBlocksNumInHost(i) = 0
            }
          }
        }
      }

      var currentHostIndex = -1
      // sort the hosts,sort key is block num
      // format: (blockNum,hostIndex)
      val blockNumWithIndex = blockNumInHost.zipWithIndex.sorted
      breakable {
        for (i <- blockNumWithIndex.indices) {
          // we chose the host which local blocks num is minium
          // and the host no be selected in this round
          // and the host still contain the local block
          if (!hostHandled.contains(hosts(blockNumWithIndex(i)._2))
            && (blockNumInHost(blockNumWithIndex(i)._2) != 0)) {
            currentHostIndex = blockNumWithIndex(i)._2
            hostHandled.append(hosts(currentHostIndex))
            break
          }
        }
      }

      if (currentHostIndex != -1) {
        val hostSelected = hosts(currentHostIndex)
        var diviedBlockNum = 0
        breakable {
          for (i <- blocksList.indices) {
            // get the local block which belong to selected host
            if (blocksList(i).getLocations.contains(hostSelected)) {
              blocksGroupby.append((hostSelected, blocksList(i)))
              diviedBlockNum += 1
              blocksList(i).getLocations.foreach { location =>
                // delete the block replication info which store in blockNumInhost
                blockNumInHost(hosts.indexOf(location)) -= 1
              }
              blocksList.remove(i)
              // first, we divide the min block num to per host
              // after all hosts were get min block num
              // (some host may record to use the no local block to reach the min block num)
              // use robin way to divide block one by one, local block will be divide first
              if (divideBlockOnebyOne) {
                break
              } else {
                if (diviedBlockNum == minBlocksNumPerHost) {
                  break
                }
              }
            }
            // check if local block is no enough
            // record how many no local block need to use to reach the min block num
            if (i == blocksList.length) {
              if (noLocalBlocksNumInHost(currentHostIndex) == 0) {
                noLocalBlocksNumInHost(currentHostIndex) = minBlocksNumPerHost - diviedBlockNum
              }
            }
          }
        }
      } else {
        for (i <- blockNumInHost.indices) {
          // when we can no get the selected host
          // check if the host which no use in this round and no local block in this host
          // record need to use no local block
          if (!hostHandled.contains(hosts(i))) {
            if (blockNumInHost(i) == 0) {
              if (divideBlockOnebyOne) {
                noLocalBlocksNumInHost(i) += 1
              } else {
                noLocalBlocksNumInHost(i) += minBlocksNumPerHost
              }
              hostHandled.append(hosts(i))
            }
          }
        }
      }

      // when all hosts were selected, reset and go to next round
      // after first round, use the robin way to divide block one by one
      if (hostHandled.length == hosts.length) {
        hostHandled.clear()
        divideBlockOnebyOne = true
      }
    }

    blocksGroupby.groupBy[String](_._1).map { iter =>
      (iter._1, iter._2.map(_._2).toArray)
    }.toArray
  }

  private def readLoadMetadataDetails(model: CarbonLoadModel, hdfsStoreLocation: String) = {
    val metadataPath = model.getCarbonDataLoadSchema.getCarbonTable.getMetaDataFilepath
    val details = CarbonUtil.readLoadMetadata(metadataPath);
    model.setLoadMetadataDetails(details.toList.asJava)
  }

  def deleteLoadsAndUpdateMetadata(
                                    carbonLoadModel: CarbonLoadModel,
                                    cube: CarbonTable, partitioner: Partitioner,
                                    hdfsStoreLocation: String,
                                    isForceDeletion: Boolean,
                                    currentRestructNumber: Integer) {
    if (LoadMetadataUtil.isLoadDeletionRequired(carbonLoadModel)) {
      val loadMetadataFilePath = CarbonLoaderUtil
        .extractLoadMetadataFileLocation(carbonLoadModel);
      val details = CarbonUtil
        .readLoadMetadata(loadMetadataFilePath);

      // Delete marked loads
      val isUpdationRequired = DeleteLoadFolders
        .deleteLoadFoldersFromFileSystem(carbonLoadModel, partitioner.partitionCount,
          hdfsStoreLocation, isForceDeletion, currentRestructNumber, details)

      if (isUpdationRequired == true) {
        // Update load metadate file after cleaning deleted nodes
        CarbonLoaderUtil.writeLoadMetadata(
          carbonLoadModel.getCarbonDataLoadSchema,
          carbonLoadModel.getDatabaseName,
          carbonLoadModel.getTableName, details.toList.asJava)
      }
    }

    CarbonDataMergerUtil
      .cleanUnwantedMergeLoadFolder(carbonLoadModel, partitioner.partitionCount, hdfsStoreLocation,
        isForceDeletion, currentRestructNumber)

  }

  // scalastyle:off
  def alterCube(
                 hiveContext: HiveContext,
                 sc: SparkContext,
                 origUnModifiedSchema: CarbonDef.Schema,
                 schema: CarbonDef.Schema,
                 schemaName: String,
                 cubeName: String,
                 hdfsStoreLocation: String,
                 addedDimensions: Seq[CarbonDef.CubeDimension],
                 addedMeasures: Seq[CarbonDef.Measure],
                 validDropDimList: ArrayBuffer[String],
                 validDropMsrList: ArrayBuffer[String],
                 curTime: Long,
                 defaultVals: Map[String, String],
                 partitioner: Partitioner): Boolean = {
    // scalastyle:on
    val cube = CarbonMetadata.getInstance().getCubeWithCubeName(cubeName, schemaName);

    val metaDataPath: String = cube.getMetaDataFilepath()

    // if there is no data loading done, no need to create RS folders
    val loadMetadataDetailsArray = CarbonUtil.readLoadMetadata(metaDataPath).toList
    if (0 == loadMetadataDetailsArray.size) {
      CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
      return true
    }

    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }

    val loadStartTime = CarbonLoaderUtil.readCurrentTime();

    val resultMap = new CarbonAlterCubeRDD(sc,
      origUnModifiedSchema,
      schema,
      schemaName,
      cubeName,
      hdfsStoreLocation,
      addedDimensions,
      addedMeasures,
      validDropDimList,
      validDropMsrList,
      curTime,
      defaultVals,
      currentRestructNumber,
      metaDataPath,
      partitioner,
      new RestructureResultImpl()).collect

    var restructureStatus: Boolean = resultMap.forall(_._2)

    if (restructureStatus) {
      if (addedDimensions.length > 0 || addedMeasures.length > 0) {
        val carbonLoadModel: CarbonLoadModel = new CarbonLoadModel()
        carbonLoadModel.setTableName(cubeName)
        carbonLoadModel.setDatabaseName(schemaName)
        carbonLoadModel.setSchema(schema);
        val metadataDetails: LoadMetadataDetails = new LoadMetadataDetails()
        CarbonLoaderUtil.recordLoadMetadata(resultMap(0)._1, metadataDetails, carbonLoadModel,
          CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS, loadStartTime)
        restructureStatus = CarbonUtil
          .createRSMetaFile(metaDataPath, "RS_" + (currentRestructNumber + 1))
      }
      if (restructureStatus) {
        CarbonEnv.getInstance(hiveContext).carbonCatalog.updateCube(schema, false)(hiveContext)
      }
    }

    restructureStatus
  }

  def dropAggregateTable(
                          sc: SparkContext,
                          schema: String,
                          cube: String,
                          partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropAggregateTableRDD(sc, kv, schema, cube, partitioner).collect
  }

  def dropCube(
                sc: SparkContext,
                schema: String,
                cube: String,
                partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropCubeRDD(sc, kv, schema, cube, partitioner).collect
  }

  def cleanFiles(
                  sc: SparkContext,
                  carbonLoadModel: CarbonLoadModel,
                  hdfsStoreLocation: String,
                  partitioner: Partitioner) {
    val cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName)
    val metaDataPath: String = cube.getMetaDataFilepath()
    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    var carbonLock = CarbonLockFactory
      .getCarbonLockObj(cube.getMetaDataFilepath(), LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        deleteLoadsAndUpdateMetadata(carbonLoadModel, cube, partitioner, hdfsStoreLocation, true,
          currentRestructNumber)
      }
    }
    finally {
      if (carbonLock.unlock()) {
        logInfo("unlock the cube metadata file successfully")
      } else {
        logError("Unable to unlock the metadata lock")
      }
    }
  }

}
