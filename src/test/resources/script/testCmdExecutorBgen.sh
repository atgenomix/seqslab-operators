osversion=$(uname -s)

if [[ $osversion == "Darwin" ]]
then
  wget https://github.com/rgcgithub/regenie/releases/download/v3.3/regenie_v3.3.gz_x86_64_OSX.zip
  unzip regenie_v3.3.gz_x86_64_OSX.zip
  mv regenie_v3.3.gz_x86_64_OSX regenie_v3.3
else
  wget https://github.com/rgcgithub/regenie/releases/download/v3.3/regenie_v3.3.gz_x86_64_Linux_mkl.zip
  unzip regenie_v3.3.gz_x86_64_Linux_mkl.zip
  mv regenie_v3.3.gz_x86_64_Linux_mkl regenie_v3.3
fi

./regenie_v3.3 \
    --step 1 \
    --bt \
    --loocv \
    --covarFile ~{covarFile} \
    --phenoFile ~{phenoFile} \
    --remove ~{removeFile} \
    --bsize 100 \
    --gz \
    --bgen ~{bgen} \
    --out fit_l0_1 \
    --run-l0 fit_parallel.master,1