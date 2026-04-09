

#extract the parameters from the python file
USER=`cat ../config/myconfig.py | grep "user" | cut -d '"' -f 2`
PASSWORD=`cat ../config/myconfig.py | grep "password" | cut -d '"' -f 2`
HOSTNAME=`cat ../config/myconfig.py | grep "hostname" | cut -d '"' -f 2`
INDEX_NAME_OUTPUT='lora-strasbourg-anonymous'
INDEX_NAME_INPUT='lora-strasbourg-full'


echo "Reindexing from $INDEX_NAME_INPUT to $INDEX_NAME_OUTPUT"





echo ""
echo ""
echo ""
echo "------ Reindexing from ${INDEX_NAME_INPUT} to ${INDEX_NAME_OUTPUT} --------"


# Reindex with the following privacy-enforcement transformations
# exclude two fields rxInfo.location and phyPayload
# convert the gatewayId into its hash
curl -k -X POST -H "Content-Type: application/json" -d '{"source":{"index":"'$INDEX_NAME_INPUT'", "_source": {"excludes":["rxInfo.location","phyPayload"]}},"dest":{"index":"'$INDEX_NAME_OUTPUT'"},"script":{"source":" ctx._source.rxInfo.gatewayId_hash  = Math.abs(ctx._source.rxInfo.remove('"'"'gatewayId'"'"').hashCode());"}}' "https://${HOSTNAME}:9200/_reindex?wait_for_completion=false" -u ${USER}:${PASSWORD}



