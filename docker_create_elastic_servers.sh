#!/bin/bash
CONTAINER_ES_NAME="elastic-server"
CONTAINER_KIB_NAME="kibana-server"

ES_VERSION="8.10.2"
KIB_VERSION="8.10.2"

CONFIG_FILE="myconfig.py"

#remove the previous image if one exists
echo "*******************************************"
echo "*            DOCKER cleanup               *"
echo "*******************************************"
for CONTAINER_NAME in $CONTAINER_ES_NAME $CONTAINER_KIB_NAME
do
    echo "--- $CONTAINER_NAME ---"
    if [ $( docker ps -a -f name=$CONTAINER_NAME | wc -l ) -eq 2 ]; then
        echo "  $CONTAINER_NAME already exists"
        echo "    ...stopping"
        docker stop $CONTAINER_NAME
        echo "    ...deleting"
        docker rm $CONTAINER_NAME
        echo "    ...deleted"
        echo ""
    fi
done


#download and run the elastic search image
echo "*******************************************"
echo "*      ELASTIC-SEARCH - instantiation     *\n"
echo "*******************************************"
docker pull docker.elastic.co/elasticsearch/elasticsearch:$ES_VERSION
docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" --detach --name $CONTAINER_ES_NAME docker.elastic.co/elasticsearch/elasticsearch:$ES_VERSION
echo ""
echo ""

#kibana
echo "*******************************************"
echo "*         KIBANA - instantiation         *"
echo "*******************************************"
docker pull docker.elastic.co/kibana/kibana:$KIB_VERSION
docker run --link elastic-server --detach -p 5601:5601 --name $CONTAINER_KIB_NAME docker.elastic.co/kibana/kibana:$KIB_VERSION
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
    ES_ADMIN_PASS=`docker exec -it $CONTAINER_ES_NAME bin/elasticsearch-reset-password -u elastic -b  | grep "New value" | cut -d ":" -f 2`
    printf ".."
done
printf "done\n"
echo "--> USER=elastic"
echo "--> PASSWORD=$ES_ADMIN_PASS"
echo "user=\"elastic\"">> $CONFIG_FILE
echo "password=\"$ES_ADMIN_PASS\"">> $CONFIG_FILE
echo "index_name=\"lora-index\"">> $CONFIG_FILE
#Enrollment key for elastic search
ES_ENROL_KEY=""
while [ -z "${ES_ENROL_KEY}" ]
do
    ES_ENROL_KEY=`docker exec -it $CONTAINER_ES_NAME bin/elasticsearch-create-enrollment-token --scope kibana`
done
echo "--> ENROL_KEY=$ES_ENROL_KEY"
echo "enroll_key=\"$ES_ENROL_KEY\"">> $CONFIG_FILE

# Verification code for Kibana
KIB_CODE_VERIF=""
while [ -z "${KIB_CODE_VERIF}" ]
do
    KIB_CODE_VERIF=`docker exec -it $CONTAINER_KIB_NAME bin/kibana-verification-code | grep "verification code"| cut -d ":" -f 2`
done
echo "--> VERIF_CODE=$KIB_CODE_VERIF"
echo "verif_code=\"$KIB_CODE_VERIF\"" >> $CONFIG_FILE
echo ""

echo "Use now your browser to open http://localhost:5601/ with the credentials above"
echo "docker exec -it $CONTAINER_KIB_NAME bin/kibana-verification-code"
