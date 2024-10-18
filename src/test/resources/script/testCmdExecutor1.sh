ls -l ~{ref}
echo ~{vcf1}
cp ~{vcf1} ~{vcfOutput1}
cp ~{vcf2} ~{vcfOutput2}
cp ~{vcf1}.tbi ~{vcfOutput1}.tbi
echo ~{drs(vcfOutput1)}
#./vep -i ~{vcf} --cache --force_overwrite --af_gnomade --vcf