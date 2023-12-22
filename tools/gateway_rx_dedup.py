# -*- coding:utf-8 -*-

""" Extract LoRaWAN Application RX """

import logging
import sys
from collections import deque
from datetime import datetime, timedelta
import pprint
import urllib3

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import config
from helpers import init_elasticsearch_cluster

LOGGER = logging.getLogger('gateway_rx_dedup')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

PP = pprint.PrettyPrinter(indent=4)
DEDUP_TIME = timedelta(milliseconds=300)

# TODO : avoid FSK message for dedup
# "term" : {"rxInfo.dataRate.modulation": "LORA"}

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
            ]

        }
    },
    "sort": [
        {config.TIME_FIELD_GW_RX:  "asc"}
    ],
}

# Disable warning for self-signed certificats
urllib3.disable_warnings()

class GatewayRxDedup(object):
    """ Parse a LoRaServer.io gateway rx document """

    def __init__(self):
        """ Constructor """
        self.conn = init_elasticsearch_cluster()
        self.tab_gw_rx = []
        self.temp_buffer = deque([])

    def refresh_buffer(self, hit):
        """
            Append new hit to the buffer and purge old hits

            https://docs.python.org/fr/2/tutorial/datastructures.html

            >>> from collections import deque
            >>> queue = deque(["Eric", "John", "Michael"])
            >>> queue.append("Terry")           # Terry arrives
            >>> queue.append("Graham")          # Graham arrives
            >>> queue.popleft()                 # The first to arrive now leaves
            'Eric'
            >>> queue.popleft()                 # The second to arrive now leaves
            'John'
            >>> queue                           # Remaining queue in order of arrival
            deque(['Michael', 'Terry', 'Graham'])
        """
        # First remove old hit > DEDUP_TIME (300ms)
        if len(self.temp_buffer) > 0:
            LOGGER.debug("len(temp_buffer)=%d", len(self.temp_buffer))
            cur_time_s = hit['_source']['rxInfo']['time']
            cur_time = self.string_to_date(cur_time_s)
            oldest_time_s = self.temp_buffer[0]['rxInfo'][0]['time']
            oldest_time = self.string_to_date(oldest_time_s)
            while (cur_time - oldest_time) > DEDUP_TIME:
                LOGGER.debug("BUFFER DROP: %s %s %s",
                             self.temp_buffer[0]['rxInfo'][0]['time'],
                             self.temp_buffer[0]['rxInfo'][0]['mac'],
                             self.temp_buffer[0]['phyPayload'])
                self.add_to_bulk_queue(self.temp_buffer[0])
                self.temp_buffer.popleft()
                if len(self.temp_buffer) == 0:
                    break
                oldest_time_s = self.temp_buffer[0]['rxInfo'][0]['time']
                oldest_time = self.string_to_date(oldest_time_s)
        # Then check if message is already in buffer (another gateway)
        dedup = False
        for msg in self.temp_buffer:
            if self.compare_msg_buffer(hit, msg) is True:
                dedup = True
                LOGGER.debug("BUFFER DUPL: %s %s %s",
                             hit['_source']['rxInfo']['time'],
                             hit['_source']['rxInfo']['mac'],
                             hit['_source']['phyPayload'])
                self.add_duplicate_msg(hit, msg)
                break
        # If payload not found in buffer, create an new msg entry
        if dedup is False:
            init_dedup_hit = self.init_gateway_rx_dedup(hit)
            LOGGER.debug("BUFFER INIT: %s %s %s",
                         init_dedup_hit['rxInfo'][0]['time'],
                         init_dedup_hit['rxInfo'][0]['mac'],
                         init_dedup_hit['phyPayload'])
            self.temp_buffer.append(init_dedup_hit)
        LOGGER.debug("##################################################################")
        self.display_buffer_content()
        LOGGER.debug("##################################################################")


    def compare_msg_buffer(self, hit, msg):
        """ Compare two gateway rx msg if same """
        if hit['_source']['phyPayload'] == msg['phyPayload'] and \
            hit['_source']['rxInfo']['frequency'] == msg['frequency'] and \
            hit['_source']['rxInfo']['dataRate']['bandwidth'] == msg['dataRate']['bandwidth'] and \
            hit['_source']['rxInfo']['dataRate']['spreadFactor'] == msg['dataRate']['spreadFactor'] and \
            hit['_source']['rxInfo']['codeRate'] == msg['codeRate']:
            return True
        else:
            return False

    def add_duplicate_msg(self, hit, msg):
        """ Add rxInfo from duplicate hit to original entry msg """
        src_rxinfo = hit['_source']['rxInfo']
        rxinfo = {}
        rxinfo['_id'] = hit['_id']
        if "channel" in src_rxinfo.keys():
            rxinfo['channel'] = src_rxinfo['channel']
        if "rfChain" in src_rxinfo.keys():
            rxinfo['rfChain'] = src_rxinfo['rfChain']
        rxinfo['loRaSNR'] = src_rxinfo['loRaSNR']
        rxinfo['timestamp'] = src_rxinfo['timestamp']
        rxinfo['mac'] = src_rxinfo['mac']
        rxinfo['time'] = src_rxinfo['time']
        rxinfo['rssi'] = src_rxinfo['rssi']
        rxinfo['crcStatus'] = src_rxinfo['crcStatus']
        msg['rxInfo'].append(rxinfo)

    def display_buffer_content(self):
        """ Print content of msg buffer """
        for msg in self.temp_buffer:
            LOGGER.debug("%s", msg['phyPayload'])
            for info in msg['rxInfo']:
                LOGGER.debug("   %s %s %s %s",
                             info['mac'],
                             info['time'],
                             info['rssi'],
                             info['loRaSNR'])

    def string_to_date(self, date_s):
        """ convert string to datetime """
        # Standard UTC String time format
        if "Z" in date_s:
            if "." in date_s:
                _time = datetime.strptime(date_s, "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                _time = datetime.strptime(date_s, "%Y-%m-%dT%H:%M:%SZ")
        # Wrong time format with Z missing, (earlier Strataggem peering)
        else:
            if "." in date_s:
                _time = datetime.strptime(date_s, "%Y-%m-%dT%H:%M:%S.%f")
            else:
                _time = datetime.strptime(date_s, "%Y-%m-%dT%H:%M:%S")
        return _time

    def init_gateway_rx_dedup(self, hit):
        """ Init Gateway RX deduplicated document """
        doc = {}
        doc['phyPayload'] = hit['_source']['phyPayload']

        src_rxinfo = hit['_source']['rxInfo']

        doc['antenna'] = src_rxinfo['antenna']
        doc['dataRate'] = src_rxinfo['dataRate']
        doc['frequency'] = src_rxinfo['frequency']
        doc['board'] = src_rxinfo['board']
        if "size" in src_rxinfo.keys():
            doc['size'] = src_rxinfo['size']
        doc['codeRate'] = src_rxinfo['codeRate']
        doc['rxInfo'] = []
        rxinfo = {}
        rxinfo['_id'] = hit['_id']
        if "channel" in src_rxinfo.keys():
            rxinfo['channel'] = src_rxinfo['channel']
        if "rfChain" in src_rxinfo.keys():
            rxinfo['rfChain'] = src_rxinfo['rfChain']
        rxinfo['loRaSNR'] = src_rxinfo['loRaSNR']
        rxinfo['timestamp'] = src_rxinfo['timestamp']
        rxinfo['mac'] = src_rxinfo['mac']
        rxinfo['time'] = src_rxinfo['time']
        rxinfo['rssi'] = src_rxinfo['rssi']
        rxinfo['crcStatus'] = src_rxinfo['crcStatus']
        doc['rxInfo'].append(rxinfo)
        return doc

    def empty_buffer_queue(self):
        """ Empty remaining message in buffer queue after end of search scroll """
        while len(self.temp_buffer) > 0:
            self.add_to_bulk_queue(self.temp_buffer[0])
            self.temp_buffer.popleft()

    def add_to_bulk_queue(self, doc):
        """ Add doc to bulk queue """
        new_doc = {"_index": config.INDEX_DEDUP_DST,
                   "_type": "gateway_rx_dedup",
                   "_id": doc['rxInfo'][0]['_id'],
                   "_source": doc}
        self.tab_gw_rx.append(new_doc)

    def parse(self, index, body):
        """ Parse documents from index """
        docs_count = 0
        bulk_count = 0

        dedup_rxinfo_count = 0
        dedup_count = 0

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
                LOGGER.debug("*************************************")
                LOGGER.debug("HIT: %s %s %s %s %s",
                             hit['_source']['phyPayload'],
                             hit['_source']['rxInfo']['mac'],
                             hit['_source']['rxInfo']['time'],
                             hit['_source']['rxInfo']['rssi'],
                             hit['_source']['rxInfo']['loRaSNR'])
                LOGGER.debug("*************************************")
                self.refresh_buffer(hit)

            # Print stats
            LOGGER.info("page_size: %d", page_size)
            LOGGER.info("(%d/%d) docs", docs_count, total_size)

            # Update dedup stats
            for dedup in self.tab_gw_rx:
                #display_doc(dedup)
                length = len(dedup['_source']['rxInfo'])
                dedup_rxinfo_count += length
                dedup_count += 1
            # Bulk update and reset buffer
            success, _ = bulk(self.conn, self.tab_gw_rx,
                              index=config.INDEX_DEDUP_DST, raise_on_error=True)
            bulk_count += success
            LOGGER.info("insert %s location lines", bulk_count)
            self.tab_gw_rx = []

            # Update page
            LOGGER.info("Scrolling...")
            page = self.conn.scroll(scroll_id=sid, scroll=config.SCROLL_TIMEOUT)
            sid = page['_scroll_id']
            page_size = len(page['hits']['hits'])
            docs_count += page_size

        # Remove last messages from buffer queue
        self.empty_buffer_queue()
        # Update dedup stats
        for dedup in self.tab_gw_rx:
            #display_doc(dedup)
            length = len(dedup['_source']['rxInfo'])
            dedup_rxinfo_count += length
            dedup_count += 1
        # Bulk update and reset buffer
        success, _ = bulk(self.conn, self.tab_gw_rx,
                          index=config.INDEX_DEDUP_DST, raise_on_error=True)
        bulk_count += success
        LOGGER.info("insert %s location lines", bulk_count)

        LOGGER.info("DEDUP_RXINFO docs %d", dedup_count)
        LOGGER.info("DEDUP_RXINFO_COUNT docs %d", dedup_rxinfo_count)

def display_doc(doc):
    """ Display document nicely with indent """
    PP.pprint(doc)

if __name__ == '__main__':
    GW_RX_DEDUP = GatewayRxDedup()
    GW_RX_DEDUP.parse(config.INDEX_DEDUP_SRC, Q_LORAWAN_GW_RX)
