set -e -o pipefail

#/usr/local/seqslab/fastp/fastp \
#  --trim_poly_g \
#  --cut_tail \
#  --thread "${thread}" \
#  --in1 "${inFileFastqR1}" \
#  --in2 "${inFileFastqR2}" \
#  --out1 "${outPathFastqR1}" \
#  --out2 "${outPathFastqR2}" \
#  --report_title "${sampleName}" \
#  --json "${outPathJson}" \
#  --html "${outPathHtml}"

cp ~{inFileFastqR1} ~{outPathFastqR1}
cp ~{inFileFastqR2} ~{outPathFastqR2}
cp ~{inFileFastqR1} ~{outPathJson}
cp ~{inFileFastqR1} ~{outPathHtml}
