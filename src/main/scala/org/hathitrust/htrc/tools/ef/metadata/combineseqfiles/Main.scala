package org.hathitrust.htrc.tools.ef.metadata.combineseqfiles

import java.io.File
import java.nio.charset.StandardCharsets
import com.gilt.gfc.time.Timer
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.hathitrust.htrc.tools.ef.metadata.combineseqfiles.Helper.logger
import play.api.libs.json.{JsObject, JsValue, Json}

import scala.io.Codec
import scala.language.reflectiveCalls
import Helper.{compress, decompress, manualBroadcastHashJoin}
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions.RddWithTryFunctions
import org.hathitrust.htrc.tools.spark.utils.Helper.stopSparkAndExit

object Main {
  val appName: String = "combine-features-metadata"

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args.toIndexedSeq)
    val featuresPath = conf.featuresPath().toString
    val metadataPath = conf.metadataPath().toString
    val outputPath = conf.outputPath().toString
    val saveAsSeqFile = conf.saveAsSeqFile()

    // set up logging destination
    conf.sparkLog.foreach(System.setProperty("spark.logFile", _))
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    implicit val codec: Codec = Codec.UTF8

    // set up Spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName(appName)
    sparkConf.setIfMissing("spark.master", "local[*]")
    val sparkMaster = sparkConf.get("spark.master")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    val numPartitions = conf.numPartitions.getOrElse(sc.defaultMinPartitions)

    try {
      logger.info("Starting...")
      logger.info(s"Spark master: $sparkMaster")

      logger.info("Metadata path: {}", metadataPath)
      logger.info("Features path: {}", featuresPath)

      // record start time
      val t0 = System.nanoTime()

      conf.outputPath().mkdirs()

      //    val featuresRDD = sc.sequenceFile[String, String](featuresPath)
      //    val metaRDD = sc.sequenceFile[String, String](metadataPath)
      //
      //    manualBroadcastHashJoin(featuresRDD, metaRDD.mapValues(compress))
      //      .map { case (htid, (features, metaBytes)) =>
      //        val jsonFeatures = Json.parse(features).as[JsObject]
      //        val jsonMeta = Json.parse(decompress(metaBytes)).as[JsObject]
      //        val jsonEF = Json.obj(
      //          "@context" -> (jsonMeta \ "@context").as[String],
      //          "schemaVersion" -> "https://schemas.hathitrust.org/EF_Schema_v_3.0",
      //          "id" -> Helper.baseIdFormat.format(Helper.datePublished, htid),
      //          "htid" -> htid,
      //          "type" -> "DataFeed",
      //          "publisher" -> Helper.publisher,
      //          "datePublished" -> Helper.datePublished,
      //          "metadata" -> (jsonMeta \ "metadata").as[JsObject],
      //          "features" -> Json.obj(
      //            "schemaVersion" -> (jsonFeatures \ "schemaVersion").as[String],
      //            "id" -> (jsonFeatures \ "id").as[String],
      //            "type" -> (jsonFeatures \ "type").as[String],
      //            //              "creator" -> (jsonFeatures \ "creator").as[JsObject],
      //            "dateCreated" -> (jsonFeatures \ "dateCreated").as[String].take(10).replaceAll("-", "").toInt,
      //            "pageCount" -> (jsonFeatures \ "pageCount").as[Int],
      //            "pages" -> (jsonFeatures \ "pages").as[JsValue]
      //          )
      //        )
      //        htid -> jsonEF.toString()
      //      }
      //      .saveAsSequenceFile(outputPath + "/output", Some(classOf[org.apache.hadoop.io.compress.BZip2Codec]))

      val joinedRDD =
        sc
          .sequenceFile[String, String](featuresPath)
          .join(sc.sequenceFile[String, String](metadataPath))
//        .repartition(numPartitions)
      //      .persist(StorageLevel.DISK_ONLY)

      //logger.info("Persisted RDD count: {}", joinedRDD.count())

      val joinedErrorAggregator = new ErrorAccumulator[(String, (String, String)), String](_._1)(sc)
      val resultRDD = joinedRDD.tryMap { case (htid, (features, meta)) =>
        val jsonFeatures = Json.parse(features).as[JsObject]
        val jsonMeta = Json.parse(meta).as[JsObject]
        val jsonEF = Json.obj(
          "@context" -> (jsonMeta \ "@context").as[String],
          "schemaVersion" -> "https://schemas.hathitrust.org/EF_Schema_v_3.0",
          "id" -> Helper.baseIdFormat.format(Helper.datePublished, htid),
          "htid" -> htid,
          "type" -> "DataFeed",
          "publisher" -> Helper.publisher,
          "datePublished" -> Helper.datePublished,
          "metadata" -> (jsonMeta \ "metadata").as[JsObject],
          "features" -> Json.obj(
            "schemaVersion" -> (jsonFeatures \ "schemaVersion").as[String],
            "id" -> (jsonFeatures \ "id").as[String],
            "type" -> (jsonFeatures \ "type").as[String],
            //              "creator" -> (jsonFeatures \ "creator").as[JsObject],
            "dateCreated" -> (jsonFeatures \ "dateCreated").as[Int],
            "pageCount" -> (jsonFeatures \ "pageCount").as[Int],
            "pages" -> (jsonFeatures \ "pages").as[List[JsObject]].map { page =>
              val language = (page \ "language").as[JsValue]
              page - "language" + ("calculatedLanguage" -> language)
            }
          )
        )
        htid -> jsonEF.toString()
      }(joinedErrorAggregator)
      //      .coalesce(numPartitions)

      if (saveAsSeqFile)
        resultRDD.saveAsSequenceFile(outputPath + "/output", Some(classOf[org.apache.hadoop.io.compress.BZip2Codec]))
      else
        resultRDD.foreach { case (id, json) =>
          val cleanId = id.replace(":", "+").replace("/", "=")
          FileUtils.writeStringToFile(new File(outputPath, s"$cleanId.json"), json, StandardCharsets.UTF_8)
        }

      //    val featuresRDD = sc.sequenceFile[String, String](featuresPath).coalesce(1000)
      //    val metadataRDD = sc.sequenceFile[String, String](metadataPath).coalesce(1000)
      //
      //    val resultRDD = featuresRDD.join(metadataRDD)
      //        .map { case (htid, (features, meta)) =>
      //          val jsonFeatures = Json.parse(features).as[JsObject]
      //          val jsonMeta = Json.parse(meta).as[JsObject]
      //          val jsonEF = Json.obj(
      //            "@context" -> (jsonMeta \ "@context").as[String],
      //            "schemaVersion" -> "https://schemas.hathitrust.org/EF_Schema_v_3.0",
      //            "id" -> Helper.baseIdFormat.format(Helper.datePublished, htid),
      //            "htid" -> htid,
      //            "type" -> "DataFeed",
      //            "publisher" -> Helper.publisher,
      //            "datePublished" -> Helper.datePublished,
      //            "metadata" -> (jsonMeta \ "metadata").as[JsObject],
      //            "features" -> Json.obj(
      //              "schemaVersion" -> (jsonFeatures \ "schemaVersion").as[String],
      //              "id" -> (jsonFeatures \ "id").as[String],
      //              "type" -> (jsonFeatures \ "type").as[String],
      ////              "creator" -> (jsonFeatures \ "creator").as[JsObject],
      //              "dateCreated" -> (jsonFeatures \ "dateCreated").as[String].take(10).replaceAll("-", "").toInt,
      //              "pageCount" -> (jsonFeatures \ "pageCount").as[Int],
      //              "pages" -> (jsonFeatures \ "pages").as[JsValue]
      //            )
      //          )
      //          htid -> jsonEF.toString()
      //        }
      //        .coalesce(numPartitions)
      //
      //    resultRDD
      //      .saveAsSequenceFile(outputPath + "/output", Some(classOf[org.apache.hadoop.io.compress.BZip2Codec]))

      if (joinedErrorAggregator.nonEmpty) {
        logger.info("Writing error report(s)...")
        joinedErrorAggregator.saveErrors(new Path(outputPath, "joinrdd_errors.txt"))
      }

      // record elapsed time and report it
      val t1 = System.nanoTime()
      val elapsed = t1 - t0

      logger.info(f"All done in ${Timer.pretty(elapsed)}")
    }
    catch {
      case e: Throwable =>
        logger.error(s"Uncaught exception", e)
        stopSparkAndExit(sc, exitCode = 500)
    }

    stopSparkAndExit(sc)
  }

}
