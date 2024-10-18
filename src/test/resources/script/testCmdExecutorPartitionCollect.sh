ls -l ~{ref}
mkdir ~{vcfOutput1}
mkdir ~{vcfOutput2}
echo "test0" > 0.txt
echo "test1" > 1.txt
echo "test2" > 2.txt
echo "test3" > 3.txt
mv 0.txt 1.txt ~{vcfOutput1}
mv 2.txt 3.txt ~{vcfOutput2}
