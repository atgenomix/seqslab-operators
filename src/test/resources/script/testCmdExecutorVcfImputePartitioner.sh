echo ~{jsonpath("$.dataset.partition().contigName")}:\
~{jsonpath("$.dataset.partition().start")}-\
~{jsonpath("$.dataset.partition().end")},\
bufferRegion:~{jsonpath("$.dataset.partition().bufferedRegion")},\
imputationRegion:~{jsonpath("$.dataset.partition().imputationRegion")} > returnValues.txt