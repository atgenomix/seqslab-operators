set -e -o pipefail
ls -l ~{write_lines(recalibration_report_filename)}
echo ~{sep=" " SampleNames}
echo ~{sep='--known-sites' known_indels_sites_indices}
cp ~{input_bam_index[0]} ~{recalibration_report_filename[0]}
cp ~{input_bam_index[1]} ~{recalibration_report_filename[1]}
