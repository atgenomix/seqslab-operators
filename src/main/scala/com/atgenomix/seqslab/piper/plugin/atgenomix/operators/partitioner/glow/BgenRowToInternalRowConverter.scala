package com.atgenomix.seqslab.piper.plugin.atgenomix.operators.partitioner.glow

import io.projectglow.common.{GlowLogging, VariantSchemas}
import io.projectglow.sql.util.RowConverter
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts [[BgenRow]]s into [[InternalRow]] with a given required schema. During construction,
 * this class will throw an [[IllegalArgumentException]] if any of the fields in the required
 * schema cannot be derived from a BGEN record.
 */
class BgenRowToInternalRowConverter(schema: StructType, hardCallsThreshold: Double)
  extends GlowLogging {
  import io.projectglow.common.VariantSchemas._

  private val converter = {
    val fns = schema.map { field =>
      val fn: RowConverter.Updater[SimpleBgenRow] = field match {
        case f if structFieldsEqualExceptNullability(f, BgenFileFormat.contigNameField) =>
          (bgen, r, i) => r.update(i, bgen.contigName)
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (bgen, r, i) => r.setLong(i, bgen.start)
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (bgen, r, i) => r.setLong(i, bgen.end)
        case f if structFieldsEqualExceptNullability(f, namesField) =>
          (bgen, r, i) => r.update(i, convertStringList(bgen.names))
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bgen, r, i) => r.update(i, UTF8String.fromString(bgen.referenceAllele))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bgen, r, i) => r.update(i, convertStringList(bgen.alternateAlleles))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          (bgen, r, i) => r.update(i, bgen.genotypes.toArray)
        case f =>
          logger.info(
            s"Column $f cannot be derived from BGEN records. It will be null for each " +
              s"row."
          )
          (_, _, _) => ()
      }
      fn
    }
    new RowConverter[SimpleBgenRow](schema, fns.toArray)
  }

  def convertRow(bgenRow: SimpleBgenRow): InternalRow = converter(bgenRow)

  private def convertStringList(strings: Seq[String]): GenericArrayData = {
    var i = 0
    val out = new Array[Any](strings.size)
    while (i < strings.size) {
      out(i) = UTF8String.fromString(strings(i))
      i += 1
    }
    new GenericArrayData(out)
  }
}
