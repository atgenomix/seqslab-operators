set -e -o pipefail
rd="~{runDate}"
rdf=${rd// /_}
echo ${rdf} > rundate.txt
interpretation_id="${rdf}_~{ID}_${CLUSTER_INIT_TIMESTAMP}"
diagnosis_id="PT123456"

echo ${interpretation_id} > iid.txt
echo ${diagnosis_id} > did.txt

echo "test" >> read_lines.txt
echo "test1" >> read_lines.txt
echo "10" > read_int.txt