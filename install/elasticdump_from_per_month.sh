#!/bin/bash
# dump per month per year for the elastic dump index
# -docker installs a docker contained (elasticdump/elasticsearch-dump) for this purpose

# example: ./elasticdump_from_per_month.sh -s login:password@ES_server_name -i lora-strasbourg-anonymous -d /tmp/data


#default values
DOCKER=0

# arguments verification
usage() {
    echo "Usage: $0 -s <ES server name> -i <index_name> -d <directory to store the dumps> -docker <false by default>" 1>&2; exit 1;
}

while getopts ":s:i:d:" option; do
    case "${option}" in
        s)
            SERVER=${OPTARG}
            ;;
        i)
            INDEX=${OPTARG}
            ;;
        d)
            DIR_RESULT=${OPTARG}
            ;;
        docker)
            DOCKER=1
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
#INDEX=lora_gateway_rx_v4
#SERVER=lora-es.icube.unistra.fr

# years & months to process
YEARS="2020 2021 2022 2023 2024 2025"
MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"

#remove previous container in case of failure
if [ "$DOCKER" -eq 1 ]
then
    docker container inspect elasticdump && docker rm elasticdump
fi
     
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
        if [ "$DOCKER" -eq 1 ]
        then
            docker run --name elasticdump --mount type=bind,source=${DIR_RESULT},target=/data --rm -ti elasticdump/elasticsearch-dump \
            --input=http://`echo $SERVER`:9200/`echo $INDEX` \
            --output=/data/`echo $INDEX`_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes \
            --searchBody="{\"query\":{  \"range\": {\"time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
#            --searchBody="{\"query\":{  \"range\": {\"mqtt_time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
        else
        echo "elasticdump \
            --input=https://`echo $SERVER`:9200/`echo $INDEX` \
            --output=/data/`echo $INDEX`_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes         "
            
            NODE_TLS_REJECT_UNAUTHORIZED=0 elasticdump \
            --input=https://`echo $SERVER`:9200/`echo $INDEX` \
            --output=${DIR_RESULT}/`echo $INDEX`_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes \
            --searchBody="{\"query\":{  \"range\": {\"time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
        fi


        # I keep only the compressed version
        tar -czvf `echo $INDEX`_data_`echo $date`.json.tar.gz `echo $INDEX`_data_`echo $date`.json
        rm `echo $INDEX`_data_`echo $date`.json
    done
done
