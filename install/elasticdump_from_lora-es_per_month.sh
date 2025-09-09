#!/bin/bash
# dump per month per year for the elastic dump index
# installs a docker contained (elasticdump/elasticsearch-dump) for this purpose

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

# NB: absolute path (required later for elastic-dump)
DIR_RESULT=`readlink -f ${DIR_RESULT}`

#test directory
if [ ! -d "$DIR_RESULT" ]; then
    echo "$DIR_RESULT is not an absolute path or the directory doesn't exist"
    exit
fi
echo "Will store the dumps into ${DIR_RESULT}"
    
#index
INDEX=lora_gateway_rx_v4


# years & months to process
YEARS="2020 2021 2022 2023 2024 2025"
MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"

#remove previous container in case of failure
docker container inspect elasticdump && docker rm elasticdump
    
     
# right location
cd $DIR_RESULT


#for year
for year in $YEARS
do
    #for month
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

        # dump runnning the docker container
        docker run --name elasticdump --mount type=bind,source=${DIR_RESULT},target=/data --rm -ti elasticdump/elasticsearch-dump \
            --input=http://lora-es.icube.unistra.fr:9200/`echo $INDEX` \
            --output=/data/`echo $INDEX`_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes \
            --searchBody="{\"query\":{  \"range\": {\"time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
#            --searchBody="{\"query\":{  \"range\": {\"mqtt_time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"

        # I keep only the compressed version
        tar -czvf `echo $INDEX`_data_`echo $date`.json.tar.gz `echo $INDEX`_data_`echo $date`.json
        rm `echo $INDEX`_data_`echo $date`.json
    done
done
