package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.transformer.glow

import htsjdk.samtools.util.Locatable

/**
  * Minimal immutable class representing a 1-based closed ended genomic interval
  * Simplified version of org.broadinstitute.hellbender.utils.SimpleInterval
  */
case class SimpleInterval(contig: String, start: Int, end: Int)
  extends Locatable with Serializable {

  if (!isValid) {
    throw new IllegalArgumentException(
      s"Invalid interval. Contig: $contig, start: $start, end: $end")
  }

  def isValid: Boolean = start > 0 && end >= start

  override def getContig: String = contig

  override def getStart: Int = start

  override def getEnd: Int = end

  def overlaps(that: SimpleInterval): Boolean = {
    this.contig == that.getContig && this.start <= that.getEnd && that.getStart <= this.end
  }

  def intersect(that: SimpleInterval): SimpleInterval = {
    if (!overlaps(that)) {
      throw new IllegalArgumentException("The two intervals need to overlap")
    }
    new SimpleInterval(getContig, math.max(getStart, that.getStart), math.min(getEnd, that.getEnd))
  }

  def spanWith(that: SimpleInterval): SimpleInterval = {
    if (getContig != that.getContig) {
      throw new IllegalArgumentException("Cannot get span for intervals on different contigs")
    }
    new SimpleInterval(contig, math.min(start, that.getStart), math.max(end, that.getEnd))
  }
}