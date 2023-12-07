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
INDEX_NAME=`cat ../config/myconfig.py | grep "index_name" | cut -d '"' -f 2`
USER=`cat ../config/myconfig.py | grep "user" | cut -d '"' -f 2`
PASSWORD=`cat ../config/myconfig.py | grep "password" | cut -d '"' -f 2`

#npm verification
if ! npm -v &> /dev/null
then
    echo "npm could not be found"
    exit 1
fi

#install npm elastic dump if required
ELASTICDUMP_BIN="../node_modules/elasticdump/bin/elasticdump"
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


    NODE_TLS_REJECT_UNAUTHORIZED=0 ./${ELASTICDUMP_BIN} \
            --output=https://${USER}:${PASSWORD}@${IP_ADDR}:9200/$INDEX_NAME \
            --input=${filename_json} \
            --type=data --limit=10000

     
    #cleaning
    rm $filename_json

done

