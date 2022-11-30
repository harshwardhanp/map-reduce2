gcloud compute networks create $1
gcloud compute firewall-rules create $1-allow-icmp --network $1 --allow icmp --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create $1-allow-ssh --network $1 --allow tcp:22 --source-ranges 0.0.0.0/0
gcloud compute firewall-rules create $1-allow-internal --network $1 --allow icmp,udp,tcp --source-ranges 10.170.0.0/16
gcloud compute firewall-rules create $1-allow-http-8080 \
    --allow tcp:8080 \
    --source-ranges 0.0.0.0/0 \
    --target-tags http-server \
    --description "Allow port 8080 access to http-server"