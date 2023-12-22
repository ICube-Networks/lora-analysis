# -*- coding:utf-8 -*-

""" Get usage statistics from LoRaWAN Gateway RX """

import json
import logging
import sys
import urllib3

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import config
from helpers import init_elasticsearch_cluster

import lorawan_operators

LOGGER = logging.getLogger('gateway_rx_stats')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

Q_CARDINALITY_DEVEUI = {
    "size": 0,
    "aggs": {
        "deveui": {
            "range": {
                "field": config.TIME_FIELD_GW_RX,
                "ranges": [
                    {"from": config.TIME_START, "to": config.TIME_END}
                ]
            },
            "aggs": {
                "type_count": {
                    "cardinality": {
                        "field": "lorawanInfos.phyPayload.macPayload.devEUI.keyword"
                    }
                }
            }
        }
    },
    "sort": [
        {
            config.TIME_FIELD_GW_RX: "asc"
        }
    ]
}

# Disable warning for self-signed certificats
urllib3.disable_warnings()

class GatewayRxStats(object):
    """
        Get usage statistics from LoRaWAN Gateway RX from LoRaServer.io documents
        stored in an Elasticsearch database
    """

    def __init__(self):
        """ Constructor """
        self.conn = init_elasticsearch_cluster()
        self.tab_gw_rx = {}

    def parse(self, index, body):
        """ Parse documents from index """
        result = self.conn.search(index=index,
                                  body=body
                                 )
        print result

    def parse_aggs_loop(self, field):
        """ loop partition of an aggs field search """
        results = []
        partition_total = 3
        for part_index_current in range(0, partition_total):
            partition = self.parse_aggs(index=config.INDEX_DEDUP_DST,
                                        partition_index=part_index_current,
                                        partition_total=partition_total,
                                        size=20000,
                                        field=field
                                       )
            for res in partition['aggregations']['lora']['buckets'][0]['item']['buckets']:
                results.append(res)
        results.sort(key=sort_by_key)
        return results

    def parse_aggs(self, index, partition_index, partition_total, size, field):
        """
            Aggs field search with partitioned result

            Examples:

            field="lorawanInfos.phyPayload.macPayload.fhdr.devAddr.keyword"
            field="lorawanInfos.phyPayload.macPayload.devEUI.keyword"
        """
        time_start = config.TIME_START
        time_end = config.TIME_END
        body = {
            "size": 0,
            "query": {
                "term" : {"lorawanInfos.phyPayload.mhdr.major": 0}
            },
            "aggs": {
                "lora": {
                    "range": {
                        "field": config.TIME_FIELD_GW_RX,
                        "ranges": [
                            {"from": '{0}'.format(time_start), "to": '{0}'.format(time_end)}
                        ]
                    },
                    "aggs" : {
                        "item" : {
                            "terms" : {
                                "field" : '{0}'.format(field),
                                "include": {
                                    "partition": '{0}'.format(partition_index),
                                    "num_partitions": '{0}'.format(partition_total)
                                },
                                "size": '{0}'.format(size)
                            }
                        }
                    },
                }
            },
            "sort": [
                {config.TIME_FIELD_GW_RX:  "asc"}
            ]
        }
        partition = self.conn.search(index=index, body=body)
        return partition

def sum_msg_by_operators(devaddr_list, operators_list):
    """
        Sum total msg by LoRaWAN operators
    """
    operator_stats = {}
    for devaddr in devaddr_list:
        operator_name = lorawan_operators.find_operators(operators_list, devaddr['key'])
        if operator_name in operator_stats.keys():
            operator_stats[operator_name] += devaddr['doc_count']
        else:
            operator_stats[operator_name] = devaddr['doc_count']
    sort_stats = sorted(operator_stats.items(), key=lambda x: x[1])
    return sort_stats

def sum_devaddr_by_operators(devaddr_list, operators_list):
    """
        Sum total devAddr by LoRaWAN operators
    """
    operator_stats = {}
    for devaddr in devaddr_list:
        operator_name = lorawan_operators.find_operators(operators_list, devaddr['key'])
        if operator_name in operator_stats.keys():
            operator_stats[operator_name]['devAddr_count'] += 1
            operator_stats[operator_name]['msg_count'] += devaddr['doc_count']
            operator_stats[operator_name]['items'].append(devaddr)
        else:
            operator_stats[operator_name] = {}
            operator_stats[operator_name]['devAddr_count'] = 1
            operator_stats[operator_name]['msg_count'] = devaddr['doc_count']
            operator_stats[operator_name]['items'] = []
            operator_stats[operator_name]['items'].append(devaddr)
    sort_stats = sorted(operator_stats.items(), key=lambda x: x[1])
    return sort_stats

def sort_by_key(element):
    """ return key key """
    return element['key']

def sort_by_doc_count(element):
    """ return doc_count key """
    return element['doc_count']

def get_all_deveui(parser_gw_rx):
    """ Get all devEUI """
    deveui_list = parser_gw_rx.parse_aggs_loop(
        field="lorawanInfos.phyPayload.macPayload.devEUI.keyword")
    return deveui_list

def get_all_devaddr(parser_gw_rx):
    """ Get all devAddr """
    devaddr_list = parser_gw_rx.parse_aggs_loop(
        field="lorawanInfos.phyPayload.macPayload.fhdr.devAddr.keyword")
    return devaddr_list

def get_all_appeui(parser_gw_rx):
    """ Get all appEUI """
    appeui_list = parser_gw_rx.parse_aggs_loop(
        field="lorawanInfos.phyPayload.macPayload.appEUI.keyword")
    return appeui_list

def save_results(results, filename="default.json", results_dir=config.RESULTS_DIR):
    """
        Save results into a JSON file.

        results -- input dictionnary
        filename -- output filename
    """
    full_path = results_dir + "/" + filename
    try:
        with open(full_path, 'w') as output_file:
            json.dump(results, output_file, indent=4, sort_keys=True)
    except IOError:
        print("I/O error", full_path)
    output_file.close()

if __name__ == '__main__':
    GW_RX_STATS = GatewayRxStats()
    OPERATORS_LIST = lorawan_operators.load_operators_csv()
    # Get Total DevEUI
    # GW_RX_STATS.parse(config.INDEX_DEDUP_DST, Q_CARDINALITY_DEVEUI)

    # Get All devEUI
    DEVEUI_LIST = get_all_deveui(GW_RX_STATS)
    save_results(DEVEUI_LIST, filename="deveui.json")

    # Get All appEUI
    APPEUI_LIST = get_all_appeui(GW_RX_STATS)
    APPEUI_LIST.sort(key=sort_by_doc_count)
    save_results(APPEUI_LIST, filename="appeui.json")

    # Get All devAddr
    DEVADDR_LIST = get_all_devaddr(GW_RX_STATS)
    save_results(DEVADDR_LIST, filename="devaddr.json")

    # Count msg by ISP
    MSG_BY_ISP = sum_msg_by_operators(DEVADDR_LIST, OPERATORS_LIST)
    save_results(MSG_BY_ISP, filename="isp_msg_count.json")

    # Count devAddr by ISP
    DEVADDR_BY_ISP = sum_devaddr_by_operators(DEVADDR_LIST, OPERATORS_LIST)
    save_results(DEVADDR_BY_ISP, filename="isp_devaddr_count.json")
