# -*- coding:utf-8 -*-

""" Extract LoRaWAN Application RX """

import logging
import sys
import urllib3

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import config
from helpers import init_elasticsearch_cluster
from lorawan_dissector import process_phypayload

LOGGER = logging.getLogger('gateway_rx_parser')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

Q_LORAWAN_GW_RX = {
    "size": 0,
    "query": {
        "bool": {
            "filter": [
                {
                    "range": {
                        config.TIME_FIELD_GW_RX: {
                            "gte": config.TIME_START,
                            "lte": config.TIME_END
                        }
                    }
                }
            ],
            "must_not": {
                "exists": {
                    "field": "lorawanInfos"
                }
            }

        }
    },
    "sort": [
        {config.TIME_FIELD_GW_RX:  "asc"}
    ],
}

# Disable warning for self-signed certificats
urllib3.disable_warnings()

class GatewayRxParser(object):
    """ Parse a LoRaServer.io application rx document """

    def __init__(self):
        """ Constructor """
        self.conn = init_elasticsearch_cluster()
        self.tab_gw_rx = {}

    def parse(self, index, body):
        """ Parse documents from index """
        docs_count = 0
        bulk_count = 0
        self.tab_gw_rx = []
        page = self.conn.search(index=index,
                                body=body,
                                scroll=config.SCROLL_TIMEOUT,
                                size=config.SCROLL_SIZE
                               )

        sid = page['_scroll_id']
        total_size = page['hits']['total']
        LOGGER.info("total: %d", total_size)

        page_size = len(page['hits']['hits'])
        docs_count += page_size
        LOGGER.info("page_size: %d", page_size)

        while page_size > 0:
            # First process current search
            for hit in page['hits']['hits']:
                #print hit
                phypayload = hit['_source']["phyPayload"]
                #print("rxinfo_time: %s" % hit['_source']["rxInfo"]["time"])
                extra_infos = process_phypayload(phypayload)
                hit['_source']['lorawanInfos'] = extra_infos
                #print hit['_source']
                new_doc = {"_index": config.INDEX_DEDUP_DST,
                           "_type": "gateway_rx_dedup",
                           "_id": hit['_id'],
                           "_source": hit['_source']}
                self.tab_gw_rx.append(new_doc)

            # Print stats
            LOGGER.info("page_size: %d", page_size)
            LOGGER.info("(%d/%d) docs", docs_count, total_size)

            # Bulk update and reset buffer
            success, _ = bulk(self.conn, self.tab_gw_rx,
                              index=config.INDEX_DEDUP_DST, raise_on_error=True)
            bulk_count += success
            print "insert %s location lines" % bulk_count

            self.tab_gw_rx = []

            # Update page
            LOGGER.info("Scrolling...")
            page = self.conn.scroll(scroll_id=sid, scroll=config.SCROLL_TIMEOUT)
            sid = page['_scroll_id']
            page_size = len(page['hits']['hits'])
            docs_count += page_size

if __name__ == '__main__':
    GW_RX_PARSER = GatewayRxParser()
    GW_RX_PARSER.parse(config.INDEX_DEDUP_DST, Q_LORAWAN_GW_RX)
