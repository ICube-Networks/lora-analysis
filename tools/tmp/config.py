# -*- coding:utf-8 -*-

""" LoRaWAN Gateway RX Parser main configuration """

# Elasticsearch cluster configuration

"""
ES_CLUSTER = [
    {'host': 'lorainfo-es.icube.unistra.fr'}
]
ES_PORT = 8443
"""

ES_CLUSTER = [
    {'host': 'localhost'}
]
ES_PORT = 9200

# Credentials
LOGIN = 'schreiner'
PASSWORD = 'icube2018'

SCROLL_SIZE = 10000
SCROLL_TIMEOUT = "60m"

#TIME_START = "now-5y"
#TIME_END = "now"
TIME_START = "2020-04-29T00:00:00"
TIME_END = "2020-04-29T04:00:00"

# Elasticsearch index configuration
TIME_FIELD_GW_RX = "rxInfo.time"

# Dedup configuration
INDEX_DEDUP_SRC = "lora_gateway_rx"
INDEX_DEDUP_DST = "lora_gateway_rx_dedup"

# DIR configuration
RESULTS_DIR = "results"
