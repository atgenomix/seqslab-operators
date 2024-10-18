package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.Const
import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsOrdering, SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.VcfPartitionerFactory.VcfPartitioner
import com.atgenomix.seqslab.piper.plugin.atgenomix.udf.GenomePartFunc
import htsjdk.samtools.util.{AsciiWriter, IOUtil}
import htsjdk.samtools.{SAMSequenceDictionaryCodec, SAMSequenceRecord}
import htsjdk.variant.vcf.VCFFileReader
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.functions.{col, explode, lit, udf}
import org.apache.spark.sql.types.{ArrayType, LongType}
import org.apache.spark.sql.{Column, Dataset, Row, functions}

import java.io.{BufferedWriter, FileOutputStream}
import java.net.URL
import java.nio.file.Files
import scala.collection.JavaConverters.{asScalaIteratorConverter, mapAsJavaMapConverter}
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object VcfPartitionerFactory {
  class VcfPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext, partBed: java.net.URL, ref: Any) extends Transformer with SupportsPartitioner with SupportsOrdering {
    private var refSeqDict: java.net.URL = null
    private var udfName: String = null

    def writeDict(it: Iterator[SAMSequenceRecord]): URL = {
      val outputDictFile = Files.createTempFile("output", ".dict").toFile
      IOUtil.assertFileIsWritable(outputDictFile)
      val writer = new BufferedWriter(new AsciiWriter(new FileOutputStream(outputDictFile)))
      val samDictCodec = new SAMSequenceDictionaryCodec(writer)
      samDictCodec.encodeHeaderLine(false)
      for (samSequenceRecord <- it) { // retrieve aliases, if any
        samDictCodec.encodeSequenceRecord(samSequenceRecord)
      }
      writer.flush()
      outputDictFile.toURI.toURL
    }

    override def init(i: Int, i1: Int): Transformer = {

      this.refSeqDict = if (ref == null || ref.asInstanceOf[String] == "") {
        val h = operatorCtx.get("vcfHeader")
        assert(h != null, "VcfPartitioner: vcfHeader is null")
        val header = h.asInstanceOf[ArrayBuffer[String]]
        val tempVcf = Files.createTempFile("temp", ".vcf")
        Files.write(tempVcf, header.mkString("\n").getBytes)
        val dict = new VCFFileReader(tempVcf, false).getFileHeader.getSequenceDictionary
        if (dict.getSequences.size() <= 0)
          throw new RuntimeException("Unable to get SAMSequenceRecord from input file. " +
            "Try assigning ref.dict url with VcfPartitioner:refSeqDict argument")
        val url = writeDict(dict.getSequences.iterator().asScala)
        tempVcf.toFile.delete()
        url
      } else {
        val rp = getClass.getResource(ref.asInstanceOf[String])
        if (rp != null) {
          rp
        } else {
          new URL(ref.asInstanceOf[String])
        }
      }

      val partFunc: UDF2[String, Int, Array[Long]] = new GenomePartFunc(this.partBed, this.refSeqDict)
      val udf = org.apache.spark.sql.functions.udf(partFunc, ArrayType(LongType))
      this.udfName = f"${refSeqDict.getFile}-${partBed.getFile}"
      pluginCtx.piper.spark.udf.register(this.udfName, udf)
      this
    }

    private def getPartitionNumFromBed(bed: URL): Int = {
      val f = bed.openStream()
      val partNum = Source
        .fromInputStream(f)
        .getLines
        .withFilter(_.nonEmpty)
        .withFilter(!_.startsWith("#"))
        .map(x => x.split("\t")(3))
        .toSet
        .size
      f.close()
      partNum
    }

    override def numPartitions(): Int = this.getPartitionNumFromBed(this.partBed)

    override def expr(dataset: Dataset[Row]): Column = {
      col("partId")
    }

    override def partitionId(objects: AnyRef*): Integer = {
      objects.head.asInstanceOf[Int]
    }


    override def getSortExprs(dataset: Dataset[Row]): Array[Column] = {
      Array(col("col"))
    }

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val partitioner = GenomicPartitioner(Array(this.partBed), this.refSeqDict)
      val keyColumn = functions.call_udf(this.udfName, t1.col("row"), lit(Int.MinValue))
      val partitionIdUDF = udf((key: Long) => partitioner.getPartition(key))

      t1.withColumn("key", keyColumn)
        .select(explode(col("key")) :: col("row") :: Const.preservedColumns: _*)
        .withColumn("partId", partitionIdUDF(col("col")))
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}


class VcfPartitionerFactory extends TransformerSupport {

  protected val opName: String = "VcfPartitioner"
  // assign a key and its associated position in the BED file
  // e.g. Map("contigName" -> 0, "start"-> 1, "end"-> 2)
  protected val bedKey: Map[String, Int] = Map.empty

  def getBed(partBed: String): URL = {
    val rp = getClass.getResource(partBed)
    if (rp != null) {
      rp
    } else new URL(partBed)
  }

  private def linkBedKeyToValue(bedKey: Map[String, Int], ary: Array[String])
  : Map[String, String] = {
    bedKey.mapValues(value => ary(value)).map(identity)
  }

  private def generateJsonMap(bed: URL, bedKey: Map[String, Int])
  : Array[Map[String, String]] = {
    val f = bed.openStream()
    val jsonMap = Source
      .fromInputStream(f)
      .getLines
      .withFilter(_.nonEmpty)
      .withFilter(!_.startsWith("#"))
      .map { x =>
        linkBedKeyToValue(bedKey, x.split("\t"))
      }
      .toArray
    f.close()
    jsonMap
  }

  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    val ref = operatorContext.get(s"$opName:refSeqDict")
    val bed = operatorContext.get(s"$opName:partBed")
    val partBed: java.net.URL = if (bed != null) {
      getBed(bed.asInstanceOf[String])
    } else throw new IllegalArgumentException(s"$opName:partBed cannot be null")
    if (bedKey.nonEmpty) {
      val dataset: AnyRef = generateJsonMap(partBed, bedKey)
      operatorContext.setProperties(Map("dataset" -> dataset).asJava)
    }
    new VcfPartitioner(pluginContext, operatorContext, partBed, ref)
  }
}