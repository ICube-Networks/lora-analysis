# -*- coding:utf-8 -*-

""" Common functions for LoRaWAN database logs """

from elasticsearch import Elasticsearch

import config


def init_elasticsearch_cluster(host=config.ES_CLUSTER,
                               port=config.ES_PORT,
                               login=config.LOGIN,
                               password=config.PASSWORD,
                               secure=False):
    """
        Init an Elasticsearch cluster
    """
    if secure is True:
        conn = Elasticsearch(host,
                             http_auth=(login, password),
                             port=port,
                             use_ssl=True,
                             verify_certs=False,
                             timeout=30,
                             max_retries=10,
                             retry_on_timeout=True)
    else:
        conn = Elasticsearch(host,
                             port=port,
                             timeout=30,
                             max_retries=10,
                             retry_on_timeout=True)
    return conn
