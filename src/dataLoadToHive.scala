import java.io.FileInputStream
import java.util.Properties

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @author soumyakantidas007@gmail.com
  * @author rajarshisarkar21@gmail.com
  * @todo Comment the code.
  */
object dataLoadToHive {
  
  def main(args: Array[String]): Unit = {

    //These are externalized HDFS paths, read from properties file
    val prop = new Properties()
    prop.load(new FileInputStream(args(0)))
    val sourceDir = prop.getProperty("dataSource")
    val doserMalfunctionSavePath = prop.getProperty("doserMalfunctionTableLocation") + "/"
    val doserConfigSavePath = prop.getProperty("doserConfigTableLocation") + "/"
    val productionSeamSavePath = prop.getProperty("productionTableLocation") + "/"
    val baLogPath = prop.getProperty("baLogPath") + "/"
    val dosKonLogPath = prop.getProperty("dosKonLogPath") + "/"
    val seamLogPath = prop.getProperty("seamLogPath") + "/"
    val destPath = prop.getProperty("archivePath")

    val conf = new SparkConf()
      .setAppName("daimler")
      //      .setMaster("local[*]")
      .setMaster("yarn-client")

    val spark = SparkSession.builder()
      .config(conf)
      //        .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlc = spark.sqlContext
    /*val sc = new SparkContext(conf)
    val sqlc = new HiveContext(sc)*/


    Logger.getRootLogger.setLevel(Level.ERROR)

    // source directory
    val dir = sourceDir

    //
    val listOfDates = getHadoopListOfFolders(sc, dir)

    println("##############################--Program Started--###################################")

    for (date <- listOfDates) {
      val dateName = date.getPath.toString.split("/").last
      val listOfFolders = getHadoopListOfFolders(sc, date.getPath.toString)
      //folder within date folders
      for (folder <- listOfFolders) {
        val listOfFiles = getHadoopListOfFiles(sc, folder.getPath.toString)
        val folderName = folder.getPath.toString.split("/").last

        val lineNumber = folder.getPath.toString.split("/").last.substring(4, 8) // 8 is exclusive
        listOfFiles.foreach(file => {
          if (file.getPath.toString.contains("BALogFile")) {
            val fileName = file.getPath.toString.split("/").last
            val logPath = baLogPath + dateName + "_" + folderName + "_" + fileName + "_" + "BALog"
            val (rowData, errorCount) = transformFile(sc, file.getPath.toString, lineNumber)
            val message = "Writing " + dateName + "-" + folderName + "-" + "BALog"
            saveBALogToHive(sqlc, rowData, message, savePath = doserMalfunctionSavePath)
            if (errorCount.toInt != 0) {
              val logData = "Number of Malfunction details missing:\t" + errorCount + "\n\n"
              val baLogErrorRDD = sc.makeRDD(Seq(logData))
              baLogErrorRDD.saveAsTextFile(logPath + System.currentTimeMillis())
            }

          } else if (file.getPath.toString.contains("DosKonfigLog")) {
            val fileName = file.getPath.toString.split("/").last
            val logPath = dosKonLogPath + dateName + "_" + folderName + "_" + fileName + "_" + "DosLog"
            val (rowData, errorCount) = transformFile(sc, file.getPath.toString, lineNumber)
            val message = "Writing " + dateName + "-" + folderName + "-" + "DosKonfig"
            saveDosKonfigLogToHive(sqlc, rowData, message, savePath = doserConfigSavePath)
            if (errorCount.toInt != 0) {
              val logData = "Number of Doser configuration details missing:\t" + errorCount + "\n\n"
              val dosKonfigErrorRDD = sc.makeRDD(Seq(logData))
              dosKonfigErrorRDD.saveAsTextFile(logPath + System.currentTimeMillis())
            }

          } else if (file.getPath.toString.contains("MasterPrgLog")) {
            val filePath = file.getPath.toString //returns absolute path
            val fileName = filePath.split("/").last //split the absolute filepath by "/" and take the last element( eg MasterPrgLog1.txt)

            val doserNumber = fileName.substring(12, fileName.indexOf(".")) //extract the integer before .txt

            val logFileName = dateName + "_" + folderName + "_" + fileName
            val errorLogPath = seamLogPath + logFileName + "_" + "seamLog"

            val (rowData, errorRDD) = transformMasterPrgLog(sc, filePath, lineNumber, doserNumber) //rowData is our desired output. RDD[String]
            val message = "Writing " + dateName + "-" + folderName + "-" + fileName
            saveMasterprgLogToHive(sqlc, rowData, message, savePath = productionSeamSavePath)

            if (!errorRDD.isEmpty()) {
              errorRDD.saveAsTextFile(errorLogPath + System.currentTimeMillis())
            }
          }
        })
      }
      recursiveMove(sc, date.getPath.toString, destPath)
    }
  }


  /**
    * This function returns an array of FileStatus objects of the folders in the given HDFS directory.
    *
    * @param sc  : [SparkContext]
    * @param dir : [String]: Path of HDFS source-parent directory
    * @return status: [Array(FileStatus)]: Array of child directories as FileStatus objects
    */
  def getHadoopListOfFolders(sc: SparkContext, dir: String): Array[FileStatus] = {
    val status = FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(dir))
      .filter(_.isDirectory)

    status
  }

  /**
    * This function returns an array of FileStatus objects of the files in the given HDFS directory.
    *
    * @param sc  : [SparkContext]
    * @param dir : [String]: Path of HDFS directory
    * @return status: [Array(FileStatus)]: Array of files as FileStatus object
    */
  def getHadoopListOfFiles(sc: SparkContext, dir: String): Array[FileStatus] = {
    val status = FileSystem.get(sc.hadoopConfiguration)
      .listStatus(new Path(dir))
      .filter(_.isFile)

    status
  }

  /**
    * Moves a directory to another specified location
    *
    * @param sc       : [SparkContext]
    * @param path     : [String]: Original path of the directory
    * @param destPath : [String]: Destination path --> final location
    */
  def recursiveMove(sc: SparkContext, path: String, destPath: String): Unit = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.rename(new Path(path), new Path(destPath))
    //    fs.delete(new Path(path), true)
    println("Moved: " + path)
  }

  /**
    * Function takes in location of file with LineNumber specific to file, does the necessary transformations
    * to the file, returns a RDD of the transformed file and a count of error logs.
    *
    * APPLICABLE TO BaLog and DosKonfig ONLY.
    *
    * @param sc         : [SparkContext]
    * @param path       : [String]: Path to the file.
    * @param lineNumber : [String]: Parameter --> Given Line Number extracted from folder name.
    * @return (row, count): (RDD[String], Int): row --> RDD of tranformed data
    *         count --> count of missing lines
    */
  def transformFile(sc: SparkContext, path: String, lineNumber: String): (RDD[String], Int) = {
    val LogRDD = sc.textFile(path)
    val header = LogRDD.first()
    val structureSize = header.split(";;")(0).split(";").length
    val errorCountArray = ArrayBuffer.empty[Int]
    val rowData = LogRDD.filter(_ != header)
      .map(_ + "a")
      .map(line => {
        val someArray = line.split(";")
        val slicedArray = someArray.slice(0, someArray.length - 1)
        val rowList = new ListBuffer[String]
        var indexCount = 0
        while (indexCount + structureSize - 1 < slicedArray.length) {
          val currentArray = slicedArray.slice(indexCount, structureSize + indexCount)
          if (checkEmpty(currentArray)) {
            errorCountArray += 1
            indexCount += structureSize
          }
          else {
            rowList += lineNumber + currentArray.mkString(";") + ";"
            indexCount += structureSize
          }

        }
        (rowList, errorCountArray)
      })

    val row = rowData
      .map(_._1)
      .flatMap(li => li)

    val errorCount = rowData
      .map(_._2)
      .flatMap(e => e)

    val count = if (!errorCount.isEmpty()) {
      errorCount.reduce(_ + _)
    } else 0

    (row, count)
  }

  /**
    * This function saves a BaLog RDD to a specified path.
    *
    * @param sqlc     : [SQLContext]
    * @param rowData  : RDD[String]: RDD of transformed/processed data
    * @param message  : [String]: Display message during execution
    * @param savePath : [String]: HDFS destination path.
    */
  def saveBALogToHive(sqlc: SQLContext, rowData: RDD[String], message: String, savePath: String): Unit = {

    val schema = StructType(
      StructField("PROD_DOSER_LINE_NUMBER", StringType, true) ::
        StructField("PROD_DOSER_MALF_MESG_NUM", StringType, true) ::
        StructField("PROD_DOSER_MALF_DT", StringType, true) ::
        StructField("PROD_DOSER_NUMBER", StringType, true) ::
        StructField("PROD_DOSER_MALF_MESG_TYP", StringType, true) ::
        StructField("PROD_DOSER_MALF_MESG_TXT", StringType, true) ::
        StructField("PROD_DOSER_MALF_USR", StringType, true) :: Nil
    )
    val dataRDD = rowData
      .map(line => {
        val Array(lnNum, msgNum, dtTm, dsrID, msgTyp, msgTxt, usr, a) = (line + "a").split(";")
        Row(lnNum, msgNum, dtTm, dsrID, msgTyp, msgTxt, usr)
      })

    val dataDF = sqlc.createDataFrame(dataRDD, schema) // check if this also works without schema. remove row in front of tuple and remove schema
    println(message)
    /*dataDF.write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("delimiter", ";")
      .save(savePath + "/" + System.currentTimeMillis.toString)*/
    dataDF.write.mode(SaveMode.Append).option("delimiter", ";").csv(savePath + "/" + System.currentTimeMillis.toString)
  }

  /**
    * This function saves a DosKonfig RDD to a specified path.
    *
    * @param sqlc     : [SQLContext]
    * @param rowData  : RDD[String]: RDD of transformed/processed data
    * @param message  : [String]: Display message during execution
    * @param savePath : [String]: HDFS destination path.
    */
  def saveDosKonfigLogToHive(sqlc: SQLContext, rowData: RDD[String], message: String, savePath: String): Unit = {

    val schema = StructType(
      StructField("PROD_DOSER_LINE_NUMBER", StringType, true) ::
        StructField("PROD_DOSER_MESG_NUM", StringType, true) ::
        StructField("PROD_DOSER_CONFIG_DT", StringType, true) ::
        StructField("PROD_DOSER_NUMBER", StringType, true) ::
        StructField("PROD_DOSER_PART", StringType, true) ::
        StructField("PROD_DOSER_PARAMT_TYPE", StringType, true) ::
        StructField("PROD_DOSER_ORG_VAL", StringType, true) ::
        StructField("PROD_DOSER_MOD_VAL", StringType, true) ::
        StructField("PROD_DOSER_USER", StringType, true) ::
        StructField("PROD_DOSER_COMMT", StringType, true) :: Nil
    )
    val dataRDD = rowData
      .map(line => {
        val Array(lnNum, msgNum, dtTm, dsrID, dsrPrt, paramTyp, oVal, modVal, usr, chngComm, a) = (line + "a").split(";")
        Row(lnNum, msgNum, dtTm, dsrID, dsrPrt, paramTyp, oVal, modVal, usr, chngComm)
      })
    println(message)

    val dataDF = sqlc.createDataFrame(dataRDD, schema)
    /*dataDF.write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("delimiter", ";")
      .save(savePath + "/" + System.currentTimeMillis.toString)*/
    dataDF.write.mode(SaveMode.Append).option("delimiter", ";").csv(savePath + "/" + System.currentTimeMillis.toString)
  }

  /**
    * This function saves a MasterPrgLog RDD to a specified path.
    *
    * @param sqlc     : [SQLContext]
    * @param rowData  : RDD[String]: RDD of transformed/processed data
    * @param message  : [String]: Display message during execution
    * @param savePath : [String]: HDFS destination path.
    */
  def saveMasterprgLogToHive(sqlc: SQLContext, rowData: RDD[String], message: String, savePath: String): Unit = {

    val schema = StructType(
      StructField("PROD_LINE_NUMBER", StringType, true) ::
        StructField("PROD_DOSER_ID", StringType, true) ::
        StructField("PROD_SEAM_PROGM_NUM", StringType, true) ::
        StructField("PROD_SEAM_PROGM_SP_VOL", StringType, true) ::
        StructField("PROD_SEAM_PROGM_ACT_VOL", StringType, true) ::
        StructField("PROD_SEAM_PROGM_STAT", StringType, true) ::
        StructField("PROD_SEAM_DT", StringType, true) ::
        StructField("PROD_SEAM_NUM", StringType, true) ::
        StructField("PROD_SEAM_STAT", StringType, true) ::
        StructField("PROD_SEAM_SP_VOL", StringType, true) ::
        StructField("PROD_SEAM_ACT_VOL", StringType, true) ::
        StructField("PROD_SEAM_PRESU_MAX_SP", StringType, true) ::
        StructField("PROD_SEAM_PRESU_MAX_ACT", StringType, true) ::
        StructField("PROD_SEAM_PRESU_MIN_SP", StringType, true) ::
        StructField("PROD_SEAM_PRESU_MIN_ACT", StringType, true) :: Nil
    )
    val dataRDD = rowData
      .map(line => {
        val Array(lnNum, dsrID, prgNum, prgSetVol, prgActVol, prgStat, dtTm, smNum, smStat, smSetVol, smActVol, maxPrSet, maxPrAct, minPrSet, minPrAct, a) = (line + "a").split(";")

        Row(lnNum, dsrID, prgNum, prgSetVol, prgActVol, prgStat, dtTm, smNum, smStat, smSetVol, smActVol, maxPrSet, maxPrAct, minPrSet, minPrAct)
      })

    val dataDF = sqlc.createDataFrame(dataRDD, schema)
    println(message)
    /*dataDF.write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("delimiter", ";")
      .save(savePath + "/" + System.currentTimeMillis.toString)*/
    dataDF.write.mode(SaveMode.Append).option("delimiter", ";").csv(savePath + "/" + System.currentTimeMillis.toString)

  }

  /**
    * Function takes in location of file with LineNumber and doserNumber specific to file, does the necessary
    * transformations to the file, returns an RDD of the transformed file and an RDD of all error logs (seam + program).
    *
    * APPLICABLE TO MasterPrgLog ONLY.
    *
    * @param sc          : [SparkContext]
    * @param path        : [String]: Path to file
    * @param lineNumber  : [String]: Parameter 1 --> Given Line Number extracted from folder name.
    * @param doserNumber : [String]: Parameter 2 --> Given doser number extracted from file name.
    * @return (row, errorLogCompleteRDD): (RDD[String], RDD[String]): row -->  RDD of tranformed data
    *         errorLogCompleteRDD --> RDD of errors
    */
  def transformMasterPrgLog(sc: SparkContext, path: String, lineNumber: String, doserNumber: String):
  (RDD[String], RDD[String]) = {

    val dataRDD = sc.textFile(path)
    val header = dataRDD.first()
    val headerArray = header.split(";")
    val indices_Program = getProgramIndices(headerArray).toArray
    val indices_Seam = getSeamIndices(headerArray).toArray

    val rowData = dataRDD.filter(_ != header)
      .map(_ + "a")
      .map(line => {
        val someArray = line.split(";")
        val slicedArray = someArray.slice(0, someArray.size - 1)
        val (outArray, errorArray, emptyProgCount) = getOutputLine(slicedArray, indices_Program, indices_Seam)

        (outArray, errorArray, emptyProgCount)
      })

    val row = rowData
      .map(_._1)
      .flatMap(arr => arr)
      .map(line => {
        lineNumber + ";" + doserNumber + ";" + line
      })

    val errorData = rowData
      .map(_._2)
      .flatMap(e => e)
      .map(line => {
        val Array(progdata, value) = line.split("\t")
        (progdata, value.toInt)
      })
      .reduceByKey((y1, y2) => y1 + y2)
      .map(line => {
        val progDetails = "Program Details:::\t" + line._1
        val emptySeamCount = "\t:::---" + line._2 + "\tempty seams detected"

        progDetails + emptySeamCount
      })

    val emptyProgCountRDD = rowData
      .map(_._3)
      .flatMap(e => e)

    val count = "Number of Empty Programs:\t" + (if (!emptyProgCountRDD.isEmpty()) {
      emptyProgCountRDD.reduce(_ + _)
    } else 0)
    val emptyProgDataRDD = sc.makeRDD(Seq(count))
    val errorLogCompleteRDD = errorData.union(emptyProgDataRDD)


    (row, errorLogCompleteRDD)
  }

  /**
    * Returns an ArrayBuffer[Int] containing all the indices/locations of "Program number" in the header.
    *
    * APPLICABLE ONLY TO MasterPrgLog Files
    *
    * @param headerArray : Array[String]: Array of header split by ";"
    * @return indices_Program: ArrayBuffer[Int]: Array of indices
    */
  def getProgramIndices(headerArray: Array[String]): ArrayBuffer[Int] = {

    val indices_Program = ArrayBuffer.empty[Int]
    var index = 0
    while (headerArray.indexOf("Program number", index) != -1) {
      index = headerArray.indexOf("Program number", index)
      indices_Program += index
      index += 1
    }
    indices_Program += headerArray.length - 1

    indices_Program
  }

  /**
    * Returns an ArrayBuffer[Int] containing all the indices/locations of "Seam number" in the header.
    *
    * APPLICABLE ONLY TO MasterPrgLog Files
    *
    * @param headerArray : Array[String]: Array of header split by ";"
    * @return indices_Seam: ArrayBuffer[Int]: Array of indices
    */
  def getSeamIndices(headerArray: Array[String]): ArrayBuffer[Int] = {

    val indices_Seam = ArrayBuffer.empty[Int]
    var index = 0
    while (headerArray.indexOf("Seam number", index) != -1) {
      index = headerArray.indexOf("Seam number", index)
      indices_Seam += index
      index += 1
    }

    indices_Seam

  }

  /**
    * This function is ONLY called from "transformMasterPrgLog" function.
    * It transforms given line(raw-data) and returns
    *
    *   1. Desired output data array --> outArray
    *   2. Seam error log array --> errorArray
    *   3. Empty Program error log array --> emptyProgCount
    *
    * @param slicedArray     : Array[String]: Array of one line of raw data split by ";"
    * @param indices_Program : Array[Int]: indices of "Program number" in header
    * @param indices_Seam    : Array[Int]: indices of "Seam number" in header
    * @return (outArray, errorArray, emptyProgCount): (ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[Int])
    *
    */
  def getOutputLine(slicedArray: Array[String], indices_Program: Array[Int], indices_Seam: Array[Int]):
  (ArrayBuffer[String], ArrayBuffer[String], ArrayBuffer[Int]) = {

    val outArray = ArrayBuffer.empty[String]
    val errorArray = ArrayBuffer.empty[String]
    val emptyProgCount = ArrayBuffer.empty[Int]

    for (p <- 0.until(indices_Program.length - 1)) {
      val progLowLim = indices_Program(p)
      val progUpLim = indices_Program(p + 1)
      val progData = slicedArray.slice(progLowLim, progLowLim + 5)

      if (checkEmpty(progData)) {

        emptyProgCount += 1

      }
      else {
        for (s <- indices_Seam) {
          if (s > progLowLim && s < progUpLim) {
            val seamDataArr = slicedArray.slice(s, s + 8)
            if (checkEmpty(seamDataArr)) {
              errorArray += progData.mkString(";") + "\t" + 1
            } else {
              outArray += progData.mkString(";") + ";" + seamDataArr.mkString(";") + ";"
            }
          } //end if
        } //end for-loop seam-indices
      } //end else


    }
    (outArray, errorArray, emptyProgCount)
  }

  /**
    * Checks if an array is empty or not. Returns boolean
    *
    * @param DataArr : Array[String]: Array of string needed to be checked
    * @return Boolean
    */
  def checkEmpty(DataArr: Array[String]): Boolean = {
    val emptyString = Array.fill[String](DataArr.length)("")
    emptyString.deep == DataArr.deep
  }

}
