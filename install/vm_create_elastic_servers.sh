#!/bin/bash
CONTAINER_ES_NAME="elastic-server"
CONTAINER_KIB_NAME="kibana-server"

VM_MANAGER="130.79.48.218"
VM_NODES="130.79.48.219"

ES_VERSION="8.11.1"
KIB_VERSION="8.11.1"

CONFIG_FILE="../config/myconfig.py"
EMAIL="theoleyre@unistra.fr"



echo "*******************************************"
echo "*      CERTIFICATES    *"
echo "*******************************************"
ssh root@$VM_MANAGER "apt update"
ssh root@$VM_MANAGER "DEBIAN_FRONTEND=noninteractive apt-get install -y snapd dnsutils"
ssh root@$VM_MANAGER "snap install core"
ssh root@$VM_MANAGER "snap install --classic certbot"
ssh root@$VM_MANAGER "ln -s /snap/bin/certbot /usr/bin/certbot"

VM_MANAGER_HOSTNAME=`ssh root@$VM_MANAGER "nslookup $VM_MANAGER | grep name | cut -d '=' -f '2' | rev | cut -d '.' -f '2-' | rev"`
echo $VM_MANAGER_HOSTNAME
ssh root@$VM_MANAGER "certbot certonly --non-interactive --no-eff-email --agree-tos --email '$EMAIL' --standalone --preferred-challenges http -d $VM_MANAGER_HOSTNAME"

ssh root@$VM_MANAGER "rm /etc/letsencrypt/live/isrgrootx1.pem; wget http://letsencrypt.org/certs/isrgrootx1.pem --show-progress --directory-prefix=/etc/letsencrypt/live"


CERT="/etc/letsencrypt/live/$VM_MANAGER_HOSTNAME/fullchain.pem"
KEY="/etc/letsencrypt/live/$VM_MANAGER_HOSTNAME/privkey.pem"





echo "*******************************************"
echo "*      ELASTIC-SEARCH - MANAGER INSTALL    *"
echo "*******************************************"

ssh root@$VM_MANAGER "sed -i 's/127.0.0.1.*localhost/127.0.0.1       localhost.localdomain localhost/g' /etc/hosts"

#ssh root@$VM_MANAGER "echo \"127.0.0.1       localhost.localdomain localhost\" >> /etc/hosts"
ssh root@$VM_MANAGER "wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo gpg --batch --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg"
ssh root@$VM_MANAGER " echo 'deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main' |  tee /etc/apt/sources.list.d/elastic-8.x.list"
ssh root@$VM_MANAGER "apt-get update"
ssh root@$VM_MANAGER "DEBIAN_FRONTEND=noninteractive apt-get install -y apt-transport-https elasticsearch"

#change the default config of ES
ssh root@$VM_MANAGER "sed -i 's/#cluster.name:.*my-application/cluster.name: lora-cluster/g' /etc/elasticsearch/elasticsearch.yml"
ssh root@$VM_MANAGER "sed -i 's/#node.name:.*node-1\"/node.name: node-1/g' /etc/elasticsearch/elasticsearch.yml"


#automatic loading
ssh root@$VM_MANAGER "/bin/systemctl daemon-reload"
ssh root@$VM_MANAGER "/bin/systemctl enable elasticsearch.service"
ssh root@$VM_MANAGER "/bin/systemctl start elasticsearch.service"



#
echo "*******************************************"
echo "*      ELASTIC-SEARCH - MANAGER CONFIG    *"
echo "*******************************************"
ES_ADMIN_PASS=`ssh root@$VM_MANAGER "/usr/share/elasticsearch/bin/elasticsearch-reset-password -u elastic -b  | grep \"New value\" | cut -d \":\" -f 2 |sed 's/ //g'|sed 's/ //g'"`
echo "ElasticSearch admin PASSWORD=$ES_ADMIN_PASS"

#certificate of the server
ES_CERT_FINGER=`ssh root@$VM_MANAGER "echo "" |  openssl s_client -connect localhost:9200 |& openssl x509 -fingerprint -noout -sha256 | cut -d '=' -f 2 | sed 's/://g'  | tr '[:upper:]' '[:lower:]'"`
echo "Certificate fingerprint: $ES_CERT_FINGER"




#Enrollment key for elastic search
ES_ENROL_KEY=""
while [ -z "${ES_ENROL_KEY}" ]
do
    ES_ENROL_KEY=`ssh root@$VM_MANAGER "/usr/share/elasticsearch/bin/elasticsearch-create-enrollment-token --scope kibana"`
done
echo "--> ENROL_KEY=$ES_ENROL_KEY"
echo "enroll_key=\"${ES_ENROL_KEY//[$'\t\r\n ']}\"" >> $CONFIG_FILE





#kibana
echo "*******************************************"
echo "*         KIBANA - install         *"
echo "*******************************************"

ssh root@$VM_MANAGER "DEBIAN_FRONTEND=noninteractive apt-get install -y kibana"
RULE='s/#server.host:.*"localhost"/server.host: "0.0.0.0"/g'
ssh root@$VM_MANAGER "sed -i '$RULE' /etc/kibana/kibana.yml"

#certificates of letsencrypt
LINES="server.ssl.enabled: true\nserver.ssl.certificate: /etc/kibana/fullchain.pem\nserver.ssl.key: /etc/kibana/privkey.pem"
RULE='s/#server.ssl.enabled:.*false/i $LINES'
ssh root@$VM_MANAGER "sed -i '$RULE' /etc/kibana/kibana.yml"
ssh root@$VM_MANAGER "cp $CERT /etc/kibana/"
ssh root@$VM_MANAGER "cp $KEY /etc/kibana/"
ssh root@$VM_MANAGER "chmod go+r /etc/kibana/*.pem"

#daemon activation
ssh root@$VM_MANAGER "/bin/systemctl daemon-reload"
ssh root@$VM_MANAGER "/bin/systemctl enable kibana.service"
ssh root@$VM_MANAGER "/bin/systemctl start kibana.service"


# Verification code for Kibana
KIB_CODE_VERIF=""
while [ -z "${KIB_CODE_VERIF}" ]
do
    KIB_CODE_VERIF=`ssh root@$VM_MANAGER  "/usr/share/kibana/bin/kibana-verification-code | grep 'verification code' | cut -d ':' -f '2'"`
done
echo "--> VERIF_CODE=$KIB_CODE_VERIF"
echo "verif_code=\"${KIB_CODE_VERIF//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo ""



#kibana
echo "*******************************************"
echo "*             PARAMETERS TO STORE         *"
echo "*******************************************"


echo "# Parameters for the elastic search & kibana servers" > $CONFIG_FILE
echo "# These values are *private*" > $CONFIG_FILE
echo "# `date`" >> $CONFIG_FILE
echo "" >> $CONFIG_FILE


# store the variables in the CONFIG_FILE
printf "done\n"
echo "--> USER=elastic"
echo "--> PASSWORD=$ES_ADMIN_PASS"
echo "user=\"elastic\"" >> $CONFIG_FILE
echo "password=\"${ES_ADMIN_PASS//[$'\t\r\n ']}\"" >> $CONFIG_FILE  #remove the EOL, etc.
echo "index_name=\"lora-index\"" >> $CONFIG_FILE
echo "cert_fingerprint=\"${ES_CERT_FINGER//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo "hostname=\"${VM_MANAGER_HOSTNAME//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo "enrollment_key=\"${ES_ENROL_KEY//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo "verif_code=\"${KIB_CODE_VERIF//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo "echo "directory_data=\"`cd ..; pwd`/data/\"" >> $CONFIG_FILE



echo "Use now your browser to open http://$VM_MANAGER:5601/ with the credentials above"
echo "docker exec -it $CONTAINER_KIB_NAME bin/kibana-verification-code"

