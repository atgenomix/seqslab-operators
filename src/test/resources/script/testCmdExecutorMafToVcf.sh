ls -l ~{report}
bgzip -c -d ~{report} > ~{resultReport}