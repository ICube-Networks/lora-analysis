# arguments verification
usage() {
    echo "Usage: $0 -i <input index name to reindex> -v <version of the input index (3 or 4)> -f <flush (0 False/1 True) previous output index>" 1>&2; exit 1;
}

while getopts ":i:v:f:" option; do
    case "${option}" in
         v)
            VERSION=${OPTARG}
            ;;
        f)
            FLUSH=${OPTARG}
            ;;
        i)
            INDEX_NAME_INPUT=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift "$((OPTIND-1))"
if [ -z "${VERSION}" ]; then
   usage
fi
if [ -z "${INDEX_NAME_INPUT}" ]; then
   usage
fi
if [ -z "${FLUSH}" ]; then
   usage
fi


#extract the parameters from the python file
USER=`cat ../config/myconfig.py | grep "user" | cut -d '"' -f 2`
PASSWORD=`cat ../config/myconfig.py | grep "password" | cut -d '"' -f 2`
HOSTNAME=`cat ../config/myconfig.py | grep "hostname" | cut -d '"' -f 2`
INDEX_NAME_OUTPUT=`cat ../config/myconfig.py | grep "index_name" | cut -d '"' -f 2`



echo "Reindexing from $INDEX_NAME_INPUT to $INDEX_NAME_OUTPUT"
echo "Flush -> ${FLUSH} "


# flush the previous index
if [ "$FLUSH" -eq 1 ]
then
    #insecure mode since this is a self-signed certificate in ES

    #destroy the previous index
    echo ""
    echo ""
    echo "---------- Delete the previous index ${INDEX_NAME_OUTPUT} ------"
    echo ""
    curl -k -X DELETE https://${USER}:${PASSWORD}@${HOSTNAME}:9200/${INDEX_NAME_OUTPUT}

    #mapping
    echo ""
    echo ""
    echo ""
    echo "------ Mapping for the previous index ${INDEX_NAME_OUTPUT} --------"
    echo ""
    curl -k -X PUT -H "Content-Type: application/json" -d '{"mappings":{"properties":{"devAddr":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"devEui":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"mType":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"phyPayload":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"rxInfo":{"properties":{"channel":{"type":"long"},"context":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"crcStatus":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"gatewayId":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"location":{"properties":{"altitude":{"type":"long"},"latitude":{"type":"float"},"longitude":{"type":"float"},"source":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"metadata":{"properties":{"region_common_name":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"region_config_id":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"rfChain":{"type":"long"},"rssi":{"type":"long"},"snr":{"type":"long"},"time":{"type":"date"},"uplinkId":{"type":"long"}}},"time":{"type":"date"},"txInfo":{"properties":{"frequency":{"type":"long"},"modulation2":{"properties":{"lora":{"properties":{"bandwidth":{"type":"long"},"codeRate":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"spreadingFactor":{"type":"long"}}}}}}}}}}' "https://${HOSTNAME}:9200/${INDEX_NAME_OUTPUT}" -u ${USER}:${PASSWORD}
fi


echo ""
echo ""
echo ""
echo "------ Reindexing from ${INDEX_NAME_INPUT} to ${INDEX_NAME_OUTPUT} --------"

echo "version $VERSION "

# version 3 -> mapping
if [ "$VERSION" -eq 3 ]
then

    curl -k -X POST -H "Content-Type: application/json" -d '{"source":{"index":"'$INDEX_NAME_INPUT'"},"dest":{"index":"'$INDEX_NAME_OUTPUT'"},"script":{"source":"ctx._source['"'"'src_version'"'"'] = 3; ctx._source['"'"'time'"'"']  = ctx._source.remove('"'"'mqtt_time'"'"');  ctx._source.txInfo.type = ctx._source.txInfo.remove('"'"'modulation'"'"'); ctx._source.txInfo.modulation = [:]; ctx._source.txInfo.modulation.lora = ctx._source.txInfo.remove('"'"'loRaModulationInfo'"'"'); ctx._source.txInfo.modulation.type  = ctx._source.txInfo.remove('"'"'type'"'"'); if (ctx._source.txInfo.modulation.lora != null && ctx._source.txInfo.modulation.lora.codeRate != null) {ctx._source.txInfo.modulation.lora.codeRate = '"'"'C_'"'"' + ctx._source.txInfo.modulation.lora.codeRate.replace('"'"'/'"'"','"'"'_'"'"');} ctx._source.rxInfo.snr = ctx._source.rxInfo.remove('"'"'loRaSNR'"'"'); ctx._source.rxInfo.gatewayId = ctx._source.rxInfo.remove('"'"'gatewayID'"'"'); ctx._source.rxInfo.uplinkIdText = ctx._source.rxInfo.remove('"'"'uplinkID'"'"');"}}' "https://${HOSTNAME}:9200/_reindex?wait_for_completion=false" -u ${USER}:${PASSWORD}

#version 4 -> reindex without any modification, only the src_version field (v4)
elif [ "$VERSION" -eq 4 ]
then

    curl -k -X POST -H "Content-Type: application/json" -d '{"source":{"index":"'$INDEX_NAME_INPUT'"},"dest":{"index":"'$INDEX_NAME_OUTPUT'"},"script":{"source":"ctx._source['"'"'src_version'"'"'] = 4; "}}' "https://${HOSTNAME}:9200/_reindex?wait_for_completion=false" -u ${USER}:${PASSWORD}

#else -> not supported
else
    echo "Only v3 and V4 are supported, not $VERSION"
    exit 4
fi
