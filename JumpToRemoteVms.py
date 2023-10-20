# Databricks notebook source
#!/bin/sh

split_num=$1
moc_string=MOC-`date "+%m-%Y-%d"`
#moc_string=temp_MOC-09-2023

export instance1=$2

export project=ProjectId
export subnet=projects/ProjectId/regions/Zone/subnetworks/subnetID 
export zone1=$3
export image=VM-Image
export scopes=https://www.googleapis.com/auth/cloud-platform

export source_code=gs://source-code-bucket/sarima_learn
export split_path=gs://input-data-bucket/folder1/project-input/supply-chain/sarima
export gcs_workloc=$split_path/$moc_string

gsutil cp $source_code/sarima_learn_rsbp_splits.sh .

sed -i -e 's/\r$//' sarima_learn_rsbp_splits.sh
chmod a+x sarima_learn_rsbp_splits.sh

bash -x sarima_learn_rsbp_splits.sh $split_num

export no_of_inst=`ls sarima_learn_splits/* | wc -l`
final_vm=$((no_of_inst-$split_num))
echo $final_vm

##########################################

echo "Modeling started for sarima-learn::$(date +%d_%m_%Y_%H_%M_%S)"

ls sarima_learn_splits/* | cut -c21-$NF  >split_filenames.txt

gcloud compute instances bulk create --name-pattern=sarima-learn-#### --no-address --count=$no_of_inst --machine-type=$instance1 --subnet=$subnet --zone=$zone1 --image=$image --scopes=$scopes --labels=model=sarima-learn --format=json > ec2run

wait $! 

mkdir -p output error
rm -f hosts.txt instanceids.txt 
rm output/* error/*

gsutil -m cp $source_code/ec2InstanceParse.py .

sudo chmod a+x ec2InstanceParse.py

python3 ec2InstanceParse.py `pwd`/ ec2run $no_of_inst

LINECOUNTER=1

while read host;
do

FILE1INPUT="$(sed -n "${LINECOUNTER}p" split_filenames.txt)"

#gcloud config set account $service_account
nohup gcloud compute ssh $host --project=$project --zone=$zone1 --quiet --command "
rm -rf sarima; mkdir sarima; chmod 777 sarima; cd sarima
export filename=$FILE1INPUT
export gcs_path=gs://input-data-bucket/folder1/project-input/supply-chain/sarima
export gcs_workloc=$gcs_workloc
export zone=$zone1
export source_code=$source_code
mkdir output; chmod 777 output; mkdir error; chmod 777 error
gsutil -m cp $source_code/sarima_learn_add_data.sh . >cpcheck
sed -i -e 's/\r$//' sarima_learn_add_data.sh
chmod a+x sarima_learn_add_data.sh
nohup bash -x sarima_learn_add_data.sh >output/$host.out 2>error/$host.err & 
exit " >output/$host.out 2>error/$host.err &
LINECOUNTER=$((LINECOUNTER+1))
done < hosts.txt
