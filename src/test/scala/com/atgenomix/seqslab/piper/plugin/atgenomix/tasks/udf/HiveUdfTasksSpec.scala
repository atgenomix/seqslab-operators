package com.atgenomix.seqslab.piper.plugin.atgenomix.tasks.udf

import com.atgenomix.seqslab.piper.plugin.atgenomix.udf.hive.{ExceptSampleId, OpenAI, RetrievePositiveGenotype, RetrieveRsID}
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaStringObjectInspector, PrimitiveObjectInspectorFactory, StringObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StandardListObjectInspector}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters.seqAsJavaListConverter



class HiveUdfTasksSpec extends AnyFlatSpec {

  it should "make sure except_sample_id function work correctly in Hive." in {
    val genotypeInspector: ObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Seq("sampleId", "phased", "calls").asJava,
      Seq(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector],
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.asInstanceOf[ObjectInspector],
        ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)
      ).asJava)
    val genotypesInspector: StandardListObjectInspector =
      ObjectInspectorFactory.getStandardListObjectInspector(genotypeInspector)
    val removeIdsInspector: StandardListObjectInspector =
      ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    val r = new ExceptSampleId()
    val resultInspector = r.initialize(Array[ObjectInspector](genotypesInspector, removeIdsInspector))
      .asInstanceOf[StandardListObjectInspector]
    val genotypes = Seq(Seq("1", true, Seq(0, 1).asJava).asJava).asJava
    val removeIds = Seq("1").asJava
    val result = r.evaluate(Array[GenericUDF.DeferredObject](
      new GenericUDF.DeferredJavaObject(genotypes),
      new GenericUDF.DeferredJavaObject(removeIds)))
    val output = resultInspector.getList(result)
    assert(output.isEmpty)
  }

  it should "make sure retrieve_positive_genotype function work correctly in Hive." in {
    val genotypeInspector: ObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
      Seq("sampleId", "phased", "calls").asJava,
      Seq(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector],
        PrimitiveObjectInspectorFactory.javaBooleanObjectInspector.asInstanceOf[ObjectInspector],
        ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector)
      ).asJava)
    val genotypesInspector: StandardListObjectInspector =
      ObjectInspectorFactory.getStandardListObjectInspector(genotypeInspector)
    val r = new RetrievePositiveGenotype()
    val resultInspector = r.initialize(Array[ObjectInspector](genotypesInspector))
      .asInstanceOf[StandardListObjectInspector]
    val genotypes = Seq(Seq("1", true, Seq(0, 1).asJava).asJava).asJava
    val result = r.evaluate(Array[GenericUDF.DeferredObject](new GenericUDF.DeferredJavaObject(genotypes)))
    val output = resultInspector.getList(result)
    assert(output.size == 1)
  }

  it should "make sure retrieve_rs_id function work correctly in Hive." in {
    val idsInspector: StandardListObjectInspector =
      ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    val r = new RetrieveRsID()
    val resultInspector = r.initialize(Array[ObjectInspector](idsInspector)).asInstanceOf[StandardListObjectInspector]
    val ids = Seq("1", "rs_1230", "sdf").asJava
    val result = r.evaluate(Array[GenericUDF.DeferredObject](new GenericUDF.DeferredJavaObject(ids)))
    val output = resultInspector.getList(result)
    assert(output.size() == 1)
  }

  it should "query Azure OpenAI with zero shot prompt" in {
    val contents = Seq("d1", "d2", "d3").asJava

    val assistant: StringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val demo: StandardListObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    val prompt: StringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val r = new OpenAI()
    val resultInspector = r.initialize(Array[ObjectInspector](assistant, demo, prompt)).asInstanceOf[JavaStringObjectInspector]
    val result = r.evaluate(Array[GenericUDF.DeferredObject](
      new GenericUDF.DeferredJavaObject("you are a geneticist."),
      new GenericUDF.DeferredJavaObject(contents),
      new GenericUDF.DeferredJavaObject("find the relationship")
    ))
    val output = resultInspector.getPrimitiveJavaObject(result)
    assert(output.nonEmpty)
  }

  it should "query Azure OpenAI with few shot prompt" in {
    val contents = Seq(Seq("q1", "a1").asJava, Seq("q2", "a2").asJava).asJava

    val assistant: StringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val demo: StandardListObjectInspector = ObjectInspectorFactory.getStandardListObjectInspector(
      ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector))
    val prompt: StringObjectInspector = PrimitiveObjectInspectorFactory.javaStringObjectInspector
    val r = new OpenAI()
    val resultInspector = r.initialize(Array[ObjectInspector](assistant, demo, prompt)).asInstanceOf[JavaStringObjectInspector]
    val result = r.evaluate(Array[GenericUDF.DeferredObject](
      new GenericUDF.DeferredJavaObject("you are a geneticist."),
      new GenericUDF.DeferredJavaObject(contents),
      new GenericUDF.DeferredJavaObject("find the relationship")
    ))
    val output = resultInspector.getPrimitiveJavaObject(result)
    assert(output.nonEmpty)
  }
}
