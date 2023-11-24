# arguments verification
usage() {
    echo "Usage: $0 -d <directory to read the dumps>" 1>&2; exit 1;
}

while getopts ":d:" option; do
    case "${option}" in
        d)
            DIR_DUMPS=${OPTARG}
            ;;
        *)
            usage
            ;;
    esac
done
shift "$((OPTIND-1))"
if [ -z "${DIR_DUMPS}" ]; then
   usage
fi

#extract the parameters from the python file
INDEX_NAME=`cat ../scripts/myconfig.py | grep "index_name" | cut -d '"' -f 2`
USER=`cat ../scriptsmyconfig.py | grep "user" | cut -d '"' -f 2`
PASSWORD=`cat ../scriptsmyconfig.py | grep "password" | cut -d '"' -f 2`

#npm verification
if ! npm -v &> /dev/null
then
    echo "npm could not be found"
    exit 1
fi

#install npm elastic dump if required
ELASTICDUMP_BIN="node_modules/elasticdump/bin/elasticdump"
if [[ ! -x $ELASTICDUMP_BIN ]]
then
    npm install elasticdump
fi

IP_ADDR="127.0.0.1"



#original location
DIR_ORIG=`pwd`

# absolute path (required later for elastic-dump)
DIR_DUMPS="`pwd`/${DIR_DUMPS}"
echo "Will read the dumps in ${DIR_DUMPS}"
echo "----------------"
#cd ${DIR_DUMPS}


# for each dump file in the directory
for filename_targz in ${DIR_DUMPS}/*.tar.gz
do
    echo "$filename_targz"
    #remove the tar.gz extension of the filename
    filename_json="`echo "$filename_targz" | rev | cut -d '.' -f 3- | rev`"

    #decompress the file
    tar -xzf $filename_targz -C ${DIR_DUMPS}
    
    
    echo "
    NODE_TLS_REJECT_UNAUTHORIZED=0 ./${ELASTICDUMP_BIN} \
--output=https://${USER}:${PASSWORD}@${IP_ADDR}:9200/$INDEX_NAME \
--input=${filename_json} \
--type=data --limit=10000
       "
    #exit 2
    NODE_TLS_REJECT_UNAUTHORIZED=0 ./${ELASTICDUMP_BIN} \
            --output=https://${USER}:${PASSWORD}@${IP_ADDR}:9200/$INDEX_NAME \
            --input=${filename_json} \
            --type=data --limit=10000

     
    #cleaning
    rm $filename_json

done


if False
then
    docker run --name elasticdump --mount type=bind,source=${DIR_RESULT},target=/data --rm -ti elasticdump/elasticsearch-dump \
            --input=http://lora-es.icube.unistra.fr:9200/lora_gateway_rx_v3 \
            --output=/data/lora_gateway_rx_v3_data_`echo $date`.json \
            --type=data --limit=10000 --debug=yes \
            --searchBody="{\"query\":{  \"range\": {\"mqtt_time\": {\"gte\": \"`echo $date`||/M\", \"lte\": \"`echo $date`||/M\"}}}}"
fi

