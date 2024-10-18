samtools view ~{bam} > ~{bamOutput}
cp ~{bam} input.bam
samtools index input.bam