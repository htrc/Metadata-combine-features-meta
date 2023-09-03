package org.hathitrust.htrc.tools.ef.metadata.combineseqfiles

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json._

import scala.reflect.ClassTag

object Helper {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(Main.appName)

  val dateTimeFormatter: DateTimeFormatter = DateTimeFormat forPattern "yyyyMMdd"
  val datePublished: Int = DateTime.now().toString(dateTimeFormatter).toInt
  val baseIdFormat: String = "https://data.analytics.hathitrust.org/extracted-features/%d/%s"
  val publisher: JsObject = Json.obj(
    "id" -> "https://analytics.hathitrust.org",
    "type" -> "Organization",
    "name" -> "HathiTrust Research Center"
  )

  val dateCreatedTransform: Reads[JsObject] = (__ \ 'dateCreated).json.update(__.read[JsString].map(sd => JsNumber(sd.value.take(10).replaceAll("-", "").toInt)))

  def compress(s: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val os = new GZIPOutputStream(baos)
    os.write(s.getBytes(StandardCharsets.UTF_8))
    os.close()
    baos.toByteArray
  }

  def decompress(bytes: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(bytes)
    val is = new GZIPInputStream(bais)
    val s = IOUtils.toString(is, StandardCharsets.UTF_8)
    is.close()
    s
  }

  def manualBroadcastHashJoin[K: Ordering : ClassTag, V1: ClassTag, V2: ClassTag](bigRDD: RDD[(K, V1)], smallRDD: RDD[(K, V2)]): RDD[(K, (V1, V2))] = {
    val smallRDDLocal: collection.Map[K, V2] = smallRDD.collectAsMap()
    val smallRDDLocalBcast = bigRDD.sparkContext.broadcast(smallRDDLocal)

    bigRDD.mapPartitions(iter => {
      iter.flatMap {
        case (k, v1) =>
          smallRDDLocalBcast.value.get(k) match {
            case None => Seq.empty[(K, (V1, V2))]
            case Some(v2) => Seq((k, (v1, v2)))
          }
      }
    }, preservesPartitioning = true)
  }
}