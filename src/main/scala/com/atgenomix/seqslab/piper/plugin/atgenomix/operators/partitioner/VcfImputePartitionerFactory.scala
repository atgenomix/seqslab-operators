package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner
import htsjdk.samtools.util.AsciiWriter

import java.io.{BufferedWriter, FileOutputStream}
import java.net.URL
import java.nio.file.Files
import scala.io.Source


class VcfImputePartitionerFactory extends VcfPartitionerFactory {

  override protected val opName: String = "VcfImputePartitioner"
  override protected val bedKey: Map[String, Int] = Map(
    "contigName" -> 0,
    "start" -> 1,
    "end" -> 2,
    "bufferedRegion" -> 5,
    "imputationRegion" -> 6
  )
  override def getBed(partBed: String): URL = Imp5ChunkToPartBed(partBed)
  private def Imp5ChunkToPartBed(coordinates: String): URL = {
    val inputStream = super.getBed(coordinates).openStream()
    val partBed = Source
      .fromInputStream(inputStream)
      .getLines
      .withFilter(_.nonEmpty)
      .withFilter(!_.startsWith("#"))
      .map { x =>
        val ary = x.split("\t")
        val contigName = ary(1)
        val startEnd = ary(2).split(":")(1).split("-")
        val start = startEnd(0).toInt
        val end = startEnd(1)
        val nSeq = ary.slice(2, ary.length)
        (contigName,start, end, nSeq)
      }
      .toArray
      .zipWithIndex.map {
        case ((contigName,start, end, nSeq), index) =>
          // The 0 in position five aims to bypass the logic in GenomicPartitioner's apply function
          s"$contigName\t$start\t$end\t$index\t0\t${nSeq.mkString("\t")}"
      }
    inputStream.close()
    val outputPartBedFile = Files.createTempFile("partBed", ".bed").toFile
    val writer = new BufferedWriter(new AsciiWriter(new FileOutputStream(outputPartBedFile)))
    partBed.foreach(line => writer.write(line + "\n"))
    writer.close()
    outputPartBedFile.toURI.toURL
  }
}