ls -l ~{ref}
echo ~{vcf1}
cp ~{vcf1} ~{vcfOutput1}
cp ~{vcf2} ~{vcfOutput2}
#./vep -i ~{vcf} --cache --force_overwrite --af_gnomade --vcf