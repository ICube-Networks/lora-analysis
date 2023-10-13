# Installation

We provide the scripts for a docker installation of Kibana and elastic search. 
Please run: `docker_create_elastic_servers`

* the installation produces `myconfig.py` including all the parameter and credentials 

* use your browser to visit `http://localhost:5601` and use the enrollment key and the verification code provided by the script (both in the output and in `myconfig.py`)

* you can use the Kibana interface to dig into the elastic search index ``http://localhost:5601``


# Dataset 

We provide two scripts:

* `elasticdump_from_lora-es_per_month.sh` connects to the elastic search server at ICube, and creates one compressed json dump per month

* `elasticsearch_load_data.py` is a Python script to connect to the local installation and that injects the dump in the local elastic search instance


# Analysis

We have up to now:

* `SF_analysis.py` that uses the local install to make queries to analyze the spreading factor 

NB: all the scripts use `myconfig.py` to store the credentials for elastic search.