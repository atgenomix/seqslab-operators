SELECT GenotypeDT.runName, GenotypeDT.sampleId, GenotypeDT.calls, \
VariantsDT.SYMBOL, VariantsDT.SOURCE, VariantsDT.`rsID`, VariantsDT.`HGMD ID` \
FROM table1 AS GenotypeDT \
LEFT JOIN table2 AS VariantsDT \
ON \
VariantsDT.contigName = GenotypeDT.contigName \
AND VariantsDT.start = GenotypeDT.start \
AND VariantsDT.end = GenotypeDT.end \
AND VariantsDT.referenceAllele = GenotypeDT.referenceAllele \
AND VariantsDT.alternateAlleles = GenotypeDT.alternateAlleles