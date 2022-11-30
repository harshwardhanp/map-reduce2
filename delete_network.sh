gcloud compute firewall-rules delete $1-allow-icmp
gcloud compute firewall-rules delete $1-allow-ssh
gcloud compute firewall-rules delete $1-allow-internal
gcloud compute firewall-rules delete $1-allow-http-8080 
gcloud compute networks delete $1