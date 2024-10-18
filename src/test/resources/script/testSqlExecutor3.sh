SELECT *, except_sample_id(table1.genotypes, array('HG001')) AS TEST \
,retrieve_pattern("^Hi.*",table1.INFO_datasetnames) AS TEST2 \
,retrieve_pattern("l$",table1.INFO_datasetnames) AS TEST3 \
,retrieve_pattern("^H",table1.INFO_datasetnames) AS TEST4 \
,retrieve_pattern("^i",table1.INFO_datasetnames) AS TEST5 \
,retrieve_pattern(".*o.*",table1.INFO_datasetsmissingcall) AS TEST6
FROM table1 LIMIT 10