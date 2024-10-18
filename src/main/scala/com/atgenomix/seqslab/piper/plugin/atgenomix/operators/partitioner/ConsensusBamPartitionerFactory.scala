package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner

import com.atgenomix.seqslab.piper.common.Const
import com.atgenomix.seqslab.piper.common.genomics.Bam.AtgxBAM
import com.atgenomix.seqslab.piper.common.genomics.GenomicPartitioner
import com.atgenomix.seqslab.piper.plugin.api.transformer.{SupportsPartitioner, Transformer, TransformerSupport}
import com.atgenomix.seqslab.piper.plugin.api.{OperatorContext, PluginContext}
import com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.ConsensusBamPartitionerFactory.ConsensusBamPartitioner
import com.atgenomix.seqslab.piper.plugin.atgenomix.udf.GenomeConsensusPartFunc
import htsjdk.samtools.{SAMFileHeader, SAMSequenceDictionaryCodec, SAMSequenceRecord}
import htsjdk.samtools.util.{AsciiWriter, IOUtil}
import org.apache.spark.sql.api.java.UDF6
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, IntegerType}
import org.apache.spark.sql.{Column, Dataset, Row, functions}

import java.io.{BufferedWriter, FileOutputStream}
import java.net.URL
import java.nio.file.Files
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.io.Source

object ConsensusBamPartitionerFactory {
  class ConsensusBamPartitioner(pluginCtx: PluginContext, operatorCtx: OperatorContext) extends Transformer with SupportsPartitioner {
    private var refSeqDict: java.net.URL = null
    private var partBed: java.net.URL = null
    private val opName: String = "ConsensusGenericBamPartitioner"
    private var udfPartition: String = null
    private var udfOrder: String = null

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

      val header = operatorCtx.getProperties.get("bamHeader").asInstanceOf[SAMFileHeader]
      val dict = header.getSequenceDictionary

      val ref = operatorCtx.get("ConsensusBamPartitioner:refSeqDict")
      val bed = operatorCtx.get("ConsensusBamPartitioner:partBed")

      this.refSeqDict = if (ref == null || ref.asInstanceOf[String] == "") {
        if (dict.getSequences.size() <= 0)
          throw new RuntimeException("Unable to get SAMSequenceRecord from input file.  Try assigning ref.dict url with BamPartitioner:refSeqDict argument")
        writeDict(dict.getSequences.iterator().asScala)
      } else {
        val rp = getClass.getResource(ref.asInstanceOf[String])
        if (rp != null) {
          rp
        } else {
          new URL(ref.asInstanceOf[String])
        }
      }

      this.partBed = if (bed != null) {
        val rp = getClass.getResource(bed.asInstanceOf[String])
        if (rp != null)
          rp
        else
          new URL(bed.asInstanceOf[String])
      } else throw new IllegalArgumentException("ConsensusBamPartitioner:partBed cannot be null")

      val partFunc: UDF6[String, Int, Int, String, Int, Int, Array[Int]] = new GenomeConsensusPartFunc(this.partBed, this.refSeqDict)
      val udf = org.apache.spark.sql.functions.udf(partFunc, ArrayType(IntegerType))
      this.udfPartition = f"$opName-${refSeqDict.getFile}-${partBed.getFile}"
      pluginCtx.piper.spark.udf.register(this.udfPartition, udf)

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

    override def call(t1: Dataset[Row]): Dataset[Row] = {
      val partitioner = GenomicPartitioner(Array(this.partBed), this.refSeqDict)
      val getKeyUDF = udf((contig: String, pos: Int) => partitioner.getDictBasedKeyValOrNoneInterval(contig, pos))
      val alignmentEndUDF = udf((start: Int, raw: Array[Byte]) => AtgxBAM.getAlignmentEnd(start, raw))
      t1
        .withColumn("alignmentEnd", alignmentEndUDF(col("alignmentStart"), col("raw")))
        .withColumn("mateAlignmentEnd", alignmentEndUDF(col("mateAlignmentStart"), col("raw")))
        .withColumn(
          "parts",
          functions.call_udf(this.udfPartition,
            col("referenceName"),
            col("alignmentStart"),
            col("alignmentEnd"),
            col("mateReferenceName"),
            col("mateAlignmentStart"),
            col("mateAlignmentEnd")
          )
        )
        .withColumn(
          "order",
          getKeyUDF(col("referenceName"), col("alignmentStart"))
        )
        .select(col("raw") :: col("order") :: functions.explode(col("parts")) :: Const.preservedColumns: _*)
        .withColumn("partId", col("col"))
        .repartition(numPartitions(), col("partId"))
        .sortWithinPartitions(col("partId"), col("order"))
        .drop("parts", "order")
    }

    override def getOperatorContext: OperatorContext = operatorCtx

    override def close(): Unit = ()
  }
}

class ConsensusBamPartitionerFactory extends TransformerSupport {
  override def createTransformer(pluginContext: PluginContext, operatorContext: OperatorContext): Transformer = {
    new ConsensusBamPartitioner(pluginContext, operatorContext)
  }
}
