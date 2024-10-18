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
import com.atgenomix.seqslab.operators.partitioner.glow.ConverterUtils.arrayDataToStringList
import io.projectglow.common.{GlowLogging, VariantSchemas}
import org.apache.spark.sql.SQLUtils.structFieldsEqualExceptNullability
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Converts internal rows to BGEN rows. Includes logic to infer phasing and ploidy if missing (eg.
 * when converting from VCF rows with no GT field), using the number of alleles and the number of
 * posterior probabilities.
 *
 * - If phasing and ploidy are missing, we assume ploidy is defaultPloidy.
 * - If phasing is missing:
 *   - If no posterior probabilities are present, we assume phasing is defaultPhasing.
 *   - If the number of posterior probabilities matches the case that the probability represents:
 *      - Either phased or unphased data: we assume phasing is defaultPhasing.
 *      - Phased data: we assume the data is phased.
 *      - Unphased data: we assume the data is unphased.
 *      - Neither: we throw an exception
 * - If ploidy is missing:
 *   - If no posterior probabilities are present, we assume ploidy is defaultPloidy.
 *   - If phased, we try to calculate the ploidy directly.
 *   - If unphased, we try to find the ploidy between [1, maxPloidy].
 *
 * @throws IllegalStateException if phasing or ploidy cannot be inferred or a single row contains
 *                               both unphased and phased data.
 */
class InternalRowToBgenRowConverter(rowSchema: StructType,
                                    maxPloidy: Int,
                                    defaultPloidy: Int,
                                    defaultPhasing: Boolean,
                                    sampleIds: Option[Array[String]])
  extends GlowLogging {
  import io.projectglow.common.VariantSchemas._

  def convert(row: InternalRow): BgenRow = {
    val fns = rowSchema.map { field =>
      val fn: (BgenRow, InternalRow, Int) => BgenRow = field match {
        case f if structFieldsEqualExceptNullability(f, BgenFileFormat.contigNameField) =>
          (bgen, r, i) => bgen.copy(contigName = r.getInt(i))
        case f if structFieldsEqualExceptNullability(f, startField) =>
          (bgen, r, i) => bgen.copy(start = r.getLong(i))
        case f if structFieldsEqualExceptNullability(f, endField) =>
          (bgen, r, i) => bgen.copy(end = r.getLong(i))
        case f if structFieldsEqualExceptNullability(f, namesField) =>
          (bgen, r, i) => bgen.copy(names = arrayDataToStringList(r.getArray(i)))
        case f if structFieldsEqualExceptNullability(f, refAlleleField) =>
          (bgen, r, i) => bgen.copy(referenceAllele = r.getString(i))
        case f if structFieldsEqualExceptNullability(f, alternateAllelesField) =>
          (bgen, r, i) => bgen.copy(alternateAlleles = arrayDataToStringList(r.getArray(i)))
        case f if f.name == VariantSchemas.genotypesFieldName =>
          (bgen, r, i) => bgen.copy(genotypes = r.getBinary(i))
        case _ => (bgen, _, _) => bgen
      }
      fn
    }.toArray

    var bgenRow = BgenRow(
      contigName = 0,
      start = -1,
      end = -1,
      names = Seq.empty,
      referenceAllele = "",
      alternateAlleles = Seq.empty,
      genotypes = Seq.empty
    )
    var i = 0
    while (i < fns.length) {
      if (!row.isNullAt(i)) {
        bgenRow = fns(i)(bgenRow, row, i)
      }
      i += 1
    }
    bgenRow
  }
}

object ConverterUtils {
  def arrayDataToStringList(array: ArrayData): Seq[String] = {
    array.toObjectArray(StringType).map(_.asInstanceOf[UTF8String].toString)
  }
}