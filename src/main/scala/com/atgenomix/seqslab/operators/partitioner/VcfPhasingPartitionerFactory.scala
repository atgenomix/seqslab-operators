package com.atgenomix.seqslab.operators.partitioner


class VcfPhasingPartitionerFactory extends VcfPartitionerFactory {

  override protected val opName: String = "VcfPhasingPartitioner"
  override protected val bedKey: Map[String, Int] = Map("contigName" -> 0)

}