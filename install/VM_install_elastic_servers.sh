#!/bin/bash
CONTAINER_ES_NAME="elastic-server"
CONTAINER_KIB_NAME="kibana-server"

ES_VERSION="8.11.1"
KIB_VERSION="8.11.1"

CONFIG_FILE="../config/myconfig.py"



#download and run the elastic search image
echo "*******************************************"
echo "*      ELASTIC-SEARCH - instantiation     *\n"
echo "*******************************************"
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | gpg --dearmor -o /usr/share/keyrings/elasticsearch-keyring.gpg
apt-get install apt-transport-https
echo "deb [signed-by=/usr/share/keyrings/elasticsearch-keyring.gpg] https://artifacts.elastic.co/packages/8.x/apt stable main" | tee /etc/apt/sources.list.d/elastic-8.x.list

apt-get update && apt-get install elasticsearch

echo ""
echo ""

#kibana
echo "*******************************************"
echo "*         KIBANA - instantiation         *"
echo "*******************************************"
apt-get install kibana

echo ""
echo ""

#kibana
echo "*******************************************"
echo "*               CONFIGURATION             *"
echo "*******************************************"

echo "# Parameters for the elastic search & kibana servers" > $CONFIG_FILE
echo "# These values are *private*" > $CONFIG_FILE
echo "# `date`" >> $CONFIG_FILE
echo "" >> $CONFIG_FILE


#password for user elastic (admin)
printf "wait for the end of the install .."
ES_ADMIN_PASS=""
while [ -z "${ES_ADMIN_PASS}" ]
do
    ES_ADMIN_PASS=`bin/elasticsearch-reset-password -u elastic -b  | grep "New value" | cut -d ":" -f 2 |sed 's/ //g'|sed 's/ //g'`
    printf ".."
done
#ES_ADMIN_PASS=`echo "${ES_ADMIN_PASS//[$'\t\r\n ']}"`

#certificate of the server
ES_CERT_FINGER=`echo "" |  openssl s_client -connect localhost:9200 |& openssl x509 -fingerprint -noout -sha256 | cut -d '=' -f 2 | sed 's/://g'  | tr '[:upper:]' '[:lower:]'`


# store the variables in the CONFIG_FILE
printf "done\n"
echo "--> USER=elastic"
echo "--> PASSWORD=$ES_ADMIN_PASS"
echo "user=\"elastic\"" >> $CONFIG_FILE
echo "password=\"${ES_ADMIN_PASS//[$'\t\r\n ']}\"" >> $CONFIG_FILE  #remove the EOL, etc.
echo "hostname=\"localhost\"" >> $CONFIG_FILE
echo "index_name=\"lora-index\"" >> $CONFIG_FILE
echo "cert_fingerprint=\"${ES_CERT_FINGER//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo "echo "directory_data=\"`cd ..; pwd`/data/\"" >> $CONFIG_FILE


#Enrollment key for elastic search
ES_ENROL_KEY=""
while [ -z "${ES_ENROL_KEY}" ]
do
    ES_ENROL_KEY=`bin/elasticsearch-create-enrollment-token --scope kibana`
done
echo "--> ENROL_KEY=$ES_ENROL_KEY"
echo "enroll_key=\"${ES_ENROL_KEY//[$'\t\r\n ']}\"" >> $CONFIG_FILE

# Verification code for Kibana
KIB_CODE_VERIF=""
while [ -z "${KIB_CODE_VERIF}" ]
do
    KIB_CODE_VERIF=`bin/kibana-verification-code | grep "verification code"| cut -d ":" -f 2`
done
echo "--> VERIF_CODE=$KIB_CODE_VERIF"
echo "verif_code=\"${KIB_CODE_VERIF//[$'\t\r\n ']}\"" >> $CONFIG_FILE
echo ""




echo "Use now your browser to open http://localhost:5601/ with the credentials above"
echo "docker exec -it $CONTAINER_KIB_NAME bin/kibana-verification-code"
