/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.csvload;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDataLoadSchema.DimensionRelation;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.processing.etl.DataLoadingException;

import org.pentaho.di.trans.steps.textfileinput.TextFileInputField;

public final class GraphExecutionUtil {
  /**
   * Comment for <code>LOGGER</code>
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(GraphExecutionUtil.class.getName());

  private GraphExecutionUtil() {

  }

  /**
   * getCsvFileToRead
   *
   * @param csvFilePath
   * @return File
   */
  public static CarbonFile getCsvFileToRead(String csvFilePath) {
    CarbonFile csvFile =
        FileFactory.getCarbonFile(csvFilePath, FileFactory.getFileType(csvFilePath));

    CarbonFile[] listFiles = null;
    if (csvFile.isDirectory()) {
      listFiles = csvFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile pathname) {
          if (!pathname.isDirectory()) {
            if (pathname.getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION) || pathname
                .getName().endsWith(CarbonCommonConstants.CSV_FILE_EXTENSION
                    + CarbonCommonConstants.FILE_INPROGRESS_STATUS)) {
              return true;
            }
          }

          return false;
        }
      });
    } else {
      listFiles = new CarbonFile[1];
      listFiles[0] = csvFile;

    }

    return listFiles[0];
  }

  /**
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  public static TextFileInputField[] getTextInputFiles(CarbonFile csvFile,
      List<String> measureColumns, StringBuilder builder, StringBuilder measuresInCSVFile,
      String delimiter) throws DataLoadingException {
    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    FileType fileType = FileFactory.getFileType(csvFile.getAbsolutePath());

    if (!csvFile.exists()) {
      csvFile = FileFactory
          .getCarbonFile(csvFile.getAbsolutePath() + CarbonCommonConstants.FILE_INPROGRESS_STATUS,
              fileType);
    }

    try {
      fileReader = FileFactory.getDataInputStream(csvFile.getAbsolutePath(), fileType);
      bufferedReader =
          new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
      readLine = bufferedReader.readLine();
    } catch (FileNotFoundException e) {
      LOGGER.error(e, "CSV Input File not found  " + e.getMessage());
      throw new DataLoadingException("CSV Input File not found ", e);
    } catch (IOException e) {
      LOGGER.error(e, "Not able to read CSV input File  " + e.getMessage());
      throw new DataLoadingException("Not able to read CSV input File ", e);
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }

    if (null != readLine) {
      String[] columnNames = readLine.split(delimiter);
      TextFileInputField[] textFileInputFields = new TextFileInputField[columnNames.length];

      int i = 0;
      String tmpCol;
      for (String column : columnNames) {
        tmpCol = column.replaceAll("\"", "");
        builder.append(tmpCol);
        builder.append(";");
        textFileInputFields[i] = new TextFileInputField();
        textFileInputFields[i].setName(tmpCol.trim());
        textFileInputFields[i].setType(2);
        measuresInCSVFile.append(tmpCol);
        measuresInCSVFile.append(";");
        i++;
      }

      return textFileInputFields;
    }

    return null;
  }

  /**
   * @param measuresInCSVFile
   * @throws DataLoadingException
   */
  public static TextFileInputField[] getTextInputFiles(String header, StringBuilder builder,
      StringBuilder measuresInCSVFile, String delimiter) throws DataLoadingException {

    String[] columnNames = header.split(delimiter);
    TextFileInputField[] textFileInputFields = new TextFileInputField[columnNames.length];

    int i = 0;
    String tmpCol;
    for (String columnName : columnNames) {
      tmpCol = columnName.replaceAll("\"", "");
      builder.append(tmpCol);
      builder.append(";");
      textFileInputFields[i] = new TextFileInputField();
      textFileInputFields[i].setName(tmpCol.trim());
      textFileInputFields[i].setType(2);
      measuresInCSVFile.append(tmpCol);
      measuresInCSVFile.append(";");
      i++;
    }

    return textFileInputFields;

  }

  public static boolean checkIsFolder(String csvFilePath) {
    try {
      if (FileFactory.isFileExist(csvFilePath, FileFactory.getFileType(csvFilePath), false)) {
        CarbonFile carbonFile =
            FileFactory.getCarbonFile(csvFilePath, FileFactory.getFileType(csvFilePath));
        return carbonFile.isDirectory();
      }
    } catch (IOException e) {
      LOGGER.error(e,
          "Not able check path exists or not  " + e.getMessage() + "path: " + csvFilePath);
    }

    return false;
  }

  /**
   * This method update the column Name
   *
   * @param schema
   * @param tableName
   * @param schema
   */
  public static Set<String> getSchemaColumnNames(CarbonDataLoadSchema schema, String tableName) {
    Set<String> columnNames = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    String factTableName = schema.getCarbonTable().getFactTableName();
    if (tableName.equals(factTableName)) {

      List<CarbonDimension> dimensions =
          schema.getCarbonTable().getDimensionByTableName(factTableName);

      for (CarbonDimension dimension : dimensions) {

        String foreignKey = null;
        for (DimensionRelation dimRel : schema.getDimensionRelationList()) {
          for (String field : dimRel.getColumns()) {
            if (dimension.equals(field)) {
              foreignKey = dimRel.getRelation().getFactForeignKeyColumn();
              break;
            }
          }
          if (null != foreignKey) {
            break;
          }
        }
        if (null == foreignKey) {
          columnNames.add(dimension.getColName());
        } else {
          columnNames.add(foreignKey);
        }
      }

      List<CarbonMeasure> measures = schema.getCarbonTable().getMeasureByTableName(factTableName);
      for (CarbonMeasure msr : measures) {
        if (!msr.getColumnSchema().isInvisible()) {
          columnNames.add(msr.getColName());
        }
      }
    } else {
      List<CarbonDimension> dimensions = schema.getCarbonTable().getDimensionByTableName(tableName);
      for (CarbonDimension dimension : dimensions) {
        columnNames.add(dimension.getColName());
      }

      List<CarbonMeasure> measures = schema.getCarbonTable().getMeasureByTableName(tableName);
      for (CarbonMeasure msr : measures) {
        columnNames.add(msr.getColName());
      }
    }

    return columnNames;

  }

  /**
   * @param csvFilePath
   * @param columnNames
   */
  public static boolean checkHeaderExist(String csvFilePath, String[] columnNames,
      String delimiter) {

    String readLine = readCSVFile(csvFilePath);

    if (null != readLine) {
      String[] columnFromCSV = readLine.toLowerCase().split(delimiter);

      List<String> csvColumnsList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

      for (String column : columnFromCSV) {
        csvColumnsList.add(column.replaceAll("\"", ""));
      }

      for (String columns : columnNames) {
        if (csvColumnsList.contains(columns)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * @param csvFilePath
   * @return
   */
  private static String readCSVFile(String csvFilePath) {

    DataInputStream fileReader = null;
    BufferedReader bufferedReader = null;
    String readLine = null;

    try {
      fileReader =
          FileFactory.getDataInputStream(csvFilePath, FileFactory.getFileType(csvFilePath));
      bufferedReader =
          new BufferedReader(new InputStreamReader(fileReader, Charset.defaultCharset()));
      readLine = bufferedReader.readLine();

    } catch (FileNotFoundException e) {
      LOGGER.error(e, "CSV Input File not found  " + e.getMessage());
    } catch (IOException e) {
      LOGGER.error(e, "Not able to read CSV input File  " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(fileReader, bufferedReader);
    }
    return readLine;
  }

  /**
   * @param csvFilePath
   * @param columnNames
   * @return
   */
  public static boolean checkCSVAndRequestedTableColumns(String csvFilePath, String[] columnNames,
      String delimiter) {

    String readLine = readCSVFile(csvFilePath);

    if (null != readLine) {
      String[] columnFromCSV = readLine.toLowerCase().split(delimiter);

      List<String> csvColumnsList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

      for (String column : columnFromCSV) {
        csvColumnsList.add(column.replaceAll("\"", "").trim());
      }

      int count = 0;

      for (String columns : columnNames) {
        if (csvColumnsList.contains(columns.toLowerCase())) {
          count++;
        }
      }
      return (count == columnNames.length);
    }

    return false;
  }

  /**
   * get the csv header
   * @param csvFilePath
   * @param columnNames
   * @param delimiter
   * @return
   */
  public static List<String> getActualCSVHeader(String csvFilePath, String[] columnNames,
                                               String delimiter) {
    String readLine = readCSVFile(csvFilePath);

    String[] columnFromCSV = readLine.toLowerCase().split(delimiter);

    List<String> csvColumnsList = new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    for (String column : columnFromCSV) {
      csvColumnsList.add(column.replaceAll("\"", "").trim());
    }

    return csvColumnsList;
  }

  public static Set<String> getDimensionColumnNames(String dimTableName,
      CarbonDataLoadSchema schema) {
    Set<String> columnNames = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (DimensionRelation dimRel : schema.getDimensionRelationList()) {
      if (dimRel.getTableName().equals(dimTableName)) {
        for (String field : dimRel.getColumns()) {
          columnNames.add(field);
        }
        break;
      }
    }
    return columnNames;
  }
}
