package com.atgenomix.seqslab.operators.partitioner.glow

/*
 * Copyright 2019 The Glow Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.io.LittleEndianDataInputStream
import io.projectglow.common.GlowLogging
import org.apache.hadoop.fs.FSDataInputStream

import java.io.DataInput
import java.nio.charset.StandardCharsets

/**
 * Parses variant records of a BGEN file into the [[io.projectglow.common.BgenRow]] schema. The iterator assumes that
 * the input streams are currently at the beginning of a variant block.
 *
 * The `init` method should be called before reading variants to skip to an appropriate starting
 * point.
 *
 * BGEN standard: https://www.well.ox.ac.uk/~gav/bgen_format/
 *
 * This class does not currently support the entire BGEN standard. Limitations:
 * - Only layout version 2 is supported
 * - Only zlib compressions is supported
 * - Only 8, 16, and 32 bit probabilities are supported
 *
 * @param metadata BGEN header info
 * @param stream Data stream that records are read from. Must be little-endian.
 * @param underlyingStream Hadoop input stream that underlies the little-endian data stream. Only
 *                         used for 1) finding the current stream position 2) cleaning up when there
 *                         are no variants left
 * @param minPos The minimum stream position from which variant blocks can be read.
 * @param maxPos The maximum stream position from which variant blocks can be read. `hasNext` will
 *               return `false` once we've reached this position.
 */
class BgenFileIterator(metadata: BgenMetadata,
                       stream: LittleEndianDataInputStream,
                       underlyingStream: FSDataInputStream,
                       minPos: Long,
                       maxPos: Long)
  extends Iterator[SimpleBgenRow]
    with GlowLogging {

  import BgenFileIterator._

  private val genotypeReader = BgenGenotypeReader.fromCompressionType(metadata.compressionType)

  def init(): Unit = {
    while (underlyingStream.getPos < minPos) {
      skipVariantBlock()
    }
  }

  def hasNext(): Boolean = {
    val ret = underlyingStream.getPos < maxPos
    if (!ret) {
      cleanup()
    }
    ret
  }

  def next(): SimpleBgenRow = {

    val variantId = readUTF8String(stream)
    val rsid = readUTF8String(stream)
    val contigName = readUTF8String(stream)
    val start = Integer.toUnsignedLong(stream.readInt()) - 1
    val nAlleles = stream.readUnsignedShort()
    val alleles = (1 to nAlleles).map(_ => readUTF8String(stream, lengthAsInt = true))

    val genotypeBytes = genotypeReader.readGenotypeBlock(stream)
    //val rawGenotypeStream = new DataInputStream(new ByteArrayInputStream(genotypeBytes))
    //val genotypeStream = new LittleEndianDataInputStream(rawGenotypeStream)
    //val genotypes = readGenotypes(nAlleles, genotypeStream, metadata.sampleIds)

    val regex = """(\d+)""".r
    val contigNameInt = regex.findFirstMatchIn(contigName).get.group(0).toInt

    SimpleBgenRow(
      contigNameInt,
      start,
      start + alleles.head.length,
      Seq(variantId, rsid),
      alleles.head,
      alleles.tail,
      genotypeBytes
    )
  }

  /**
   * Cheaply skip over a variant block while reading as little data as possible.
   */
  private def skipVariantBlock(): Unit = {
    skipString() // variant id
    skipString() // rsid
    skipString() // contigName
    stream.readInt() // start
    val nAlleles = stream.readUnsignedShort()
    (1 to nAlleles).foreach { _ =>
      skipString(lengthAsInt = true)
    } // alleles
    val probabilityBlockSize = Integer.toUnsignedLong(stream.readInt())
    stream.skip(probabilityBlockSize) // probabilities
  }

  private def cleanup(): Unit = {
    underlyingStream.close()
  }

  private def skipString(lengthAsInt: Boolean = false): Unit = {
    val len = if (lengthAsInt) {
      stream.readInt()
    } else {
      stream.readUnsignedShort()
    }
    stream.skipBytes(len)
  }
}

object BgenFileIterator {

  /**
   * Utility function to read a UTF8 string from a data stream. Included in the companion object
   * so that it can be used by the header reader and the file iterator.
   */
  def readUTF8String(stream: DataInput, lengthAsInt: Boolean = false): String = {
    val len = if (lengthAsInt) {
      stream.readInt()
    } else {
      stream.readUnsignedShort()
    }
    val bytes = new Array[Byte](len)
    stream.readFully(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }
}

/**
 * Read a BGEN header from a data stream. Performs basic validation on the header parameters
 * according to what the reader currently supports.
 */
class BgenHeaderReader(stream: LittleEndianDataInputStream)
  extends GlowLogging {

  def readHeader(sampleIdsOpt: Option[Seq[String]] = None): BgenMetadata = {
    val variantOffset = Integer.toUnsignedLong(stream.readInt()) + 4
    val headerLength = Integer.toUnsignedLong(stream.readInt())
    val nVariantBlocks = Integer.toUnsignedLong(stream.readInt())
    val nSamples = Integer.toUnsignedLong(stream.readInt())
    val magicNumber = (1 to 4).map(n => stream.readByte())

    require(
      magicNumber == Seq('b', 'g', 'e', 'n') || magicNumber == Seq(0, 0, 0, 0),
      s"Magic bytes were neither 'b', 'g', 'e', 'n' nor 0, 0, 0, 0 ($magicNumber)"
    )

    val freeData = new Array[Byte](headerLength.toInt - 20)
    stream.readFully(freeData)

    val flags = stream.readInt()
    val compressionType = flags & 3 match {
      case 0 => SnpBlockCompression.None
      case 1 => SnpBlockCompression.Zlib
      case 2 => SnpBlockCompression.Zstd
      case n => throw new IllegalArgumentException(s"Unsupported compression setting: $n")
    }
    val layoutType = flags >> 2 & 15
    require(layoutType == 2, "Only BGEN files with layout type 2 are supported")
    val hasSampleIds = flags >> 31 & 1

    val base =
      BgenMetadata(variantOffset, nSamples, nVariantBlocks, layoutType, compressionType, None)

    if (hasSampleIds == 1) {
      addSampleIdsFromHeader(headerLength, base)
    } else if (hasSampleIds == 0 && sampleIdsOpt.isDefined) {
      logger.warn("No sample IDs were parsed from the BGEN or .sample file.")
      addSampleIdsFromFile(sampleIdsOpt.get, base)
    } else {
      base
    }
  }

  private def addSampleIdsFromHeader(headerLength: Long, base: BgenMetadata): BgenMetadata = {
    val sampleBlockLength = Integer.toUnsignedLong(stream.readInt())
    val numSamples = Integer.toUnsignedLong(stream.readInt())

    if (numSamples != base.nSamples) {
      logger.warn(
        s"BGEN number of samples in sample ID block does not match header. " +
          s"($numSamples != ${base.nSamples})"
      )
    }
    if (sampleBlockLength + headerLength >= base.firstVariantOffset) {
      logger.warn(
        s"BGEN sample block length + header length >= first variant offset. File " +
          s"appears to be malformed, but attempting to parse anyway. " +
          s"($sampleBlockLength + $headerLength >= ${base.firstVariantOffset})"
      )
    }

    base.copy(sampleIds = Option((1 to numSamples.toInt).map { _ =>
      BgenFileIterator.readUTF8String(stream)
    }.toArray))
  }

  private def addSampleIdsFromFile(sampleIds: Seq[String], base: BgenMetadata): BgenMetadata = {
    val numSamples = sampleIds.length

    if (numSamples != base.nSamples) {
      logger.warn(
        s"BGEN number of samples in .sample file does not match header. " +
          s"($numSamples != ${base.nSamples})"
      )
    }
    base.copy(sampleIds = Some(sampleIds.toArray))
  }
}

case class BgenGenotype(sampleId: Option[String],
                        phased: Option[Boolean],
                        ploidy: Option[Int],
                        posteriorProbabilities: Seq[Double])

case class SimpleBgenRow(contigName: Int,
                         start: Long,
                         end: Long,
                         names: Seq[String],
                         referenceAllele: String,
                         alternateAlleles: Seq[String],
                         genotypes: Seq[Byte])

case class BgenRow(contigName: Int,
                   start: Long,
                   end: Long,
                   names: Seq[String],
                   referenceAllele: String,
                   alternateAlleles: Seq[String],
                   genotypes: Seq[Byte])

case class BgenMetadata(firstVariantOffset: Long,
                        nSamples: Long,
                        nVariantBlocks: Long,
                        layoutType: Int,
                        compressionType: SnpBlockCompression,
                        sampleIds: Option[Array[String]])

sealed trait SnpBlockCompression
object SnpBlockCompression {
  case object None extends SnpBlockCompression
  case object Zlib extends SnpBlockCompression
  case object Zstd extends SnpBlockCompression
}
