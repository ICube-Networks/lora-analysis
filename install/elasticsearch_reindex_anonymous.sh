

#extract the parameters from the python file
USER=`cat ../config/myconfig.py | grep "user" | cut -d '"' -f 2`
PASSWORD=`cat ../config/myconfig.py | grep "password" | cut -d '"' -f 2`
HOSTNAME=`cat ../config/myconfig.py | grep "hostname" | cut -d '"' -f 2`
INDEX_NAME_OUTPUT='lora-strasbourg-anonymous'
INDEX_NAME_INPUT='lora-strasbourg'


echo "Reindexing from $INDEX_NAME_INPUT to $INDEX_NAME_OUTPUT"





echo ""
echo ""
echo ""
echo "------ Reindexing from ${INDEX_NAME_INPUT} to ${INDEX_NAME_OUTPUT} --------"



curl -k -X POST -H "Content-Type: application/json" -d '{"source":{"index":"'$INDEX_NAME_INPUT'"}},"dest":{"index":"'$INDEX_NAME_OUTPUT'"},"script":{"source":"ctx._source.remove('"'"'phyPayload'"'"'); ctx._source.rxInfo.remove('"'"'location'"'"'); ctx._source.rxInfo.gatewayId_hash  = Math.abs(ctx._source.rxInfo.remove('"'"'gatewayId'"'"').hashCode());"}}' "https://${HOSTNAME}:9200/_reindex?wait_for_completion=false" -u ${USER}:${PASSWORD}



