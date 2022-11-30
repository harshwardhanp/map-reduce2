gcloud compute instances create $1 --zone=us-west1-a 
gcloud compute instances describe $1 --zone=us-west1-a --format='get(networkInterfaces[0].networkIP)'
MY_INSTANCE_NAME=$1
ZONE=us-west1-a 

gcloud compute instances create $MY_INSTANCE_NAME \
    --image-family=debian-10 \
    --image-project=debian-cloud \
    --machine-type=g1-small \
    --metadata-from-file startup-script=startup-script.sh \
    --zone $ZONE \
    --tags http-server

# gcloud compute instances create demo-vm --zone=us-west1-a 

gcloud compute instances create demo-vm \
    --image-family=debian-10 \
    --image-project=debian-cloud \
    --machine-type=g1-small \
    --metadata-from-file startup-script=startup-script.sh \
    --zone us-west1-a \
    --tags http-server