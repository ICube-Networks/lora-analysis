#!/bin/bash
# fait un dump par mois et année de la base de données elastic dump
# nécessite d'avoir l'image docker elasticdump/elasticsearch-dump

# arguments verification
usage() {
    echo "Usage: $0 -d <directory to store the dumps>" 1>&2; exit 1;
}

while getopts ":d:" option; do
    case "${option}" in
        d)
            DIR_RESULT=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift "$((OPTIND-1))"
if [ -z "${DIR_RESULT}" ]; then
   usage
fi

# absolute path (required later for elastic-dump)
DIR_RESULT="`pwd`/${DIR_RESULT}"
echo $DIR_RESULT

#test directory
[ -d "${DIR_RESULT}" ] || mkdir "${DIR_RESULT}"
echo "Will store the dumps into ${DIR_RESULT}"



# répertoire où stocker les fichiers json
YEARS="2023"
#YEARS="2020 2021 2022 2023"
MONTHS="06 07 08 09 10 11 12"
#MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"

#remove previous container in case of failure
docker container inspect elasticdump && docker rm elasticdump
        
# se placer au bon endroit
cd $DIR_RESULT


#for year in  2021
for year in $YEARS
do
    #for month in 10 11
    for month in $MONTHS
    do
        date="`echo $year`-`echo $month`"
        
        echo ""
        echo ""
        echo "-------------------------------------"
        echo "Dump de `echo $date`"
        echo "-------------------------------------"
        echo ""
        echo ""
        echo ""

        docker run --name elasticdump --mount type=bind,source=${DIR_RESULT},target=/data --rm -ti elasticdump/elasticsearch-dump \
            --input=http://lora-es.icube.unistra.fr:9200/lora_gateway_rx_v3 \
            --output=/data/lora_gateway_rx_v3_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes \
            --searchBody="{\"query\":{  \"range\": {\"mqtt_time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
        
        # je fais une version compressée!
        tar -czvf lora_gateway_rx_v3_data_`echo $date`.json.tar.gz lora_gateway_rx_v3_data_`echo $date`.json
        
    done
done
