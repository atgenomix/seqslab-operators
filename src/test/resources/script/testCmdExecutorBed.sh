#echo ~{vcf}
#cp ~{vcf} ~{vcfOutput}
cp ~{bed} ~{bedOutput}
cp ~{bed2} ~{bedOutput2}
#./vep -i ~{vcf} --cache --force_overwrite --af_gnomade --vcf