# Snaphot Elastic Search

This dataset contains all the LoRa packets captured from Oct. 2020 to August 2025 in Strasbourg, France. You can refer to [https://inetlab.icube.unistra.fr/index.php/LRP_IoT](https://inetlab.icube.unistra.fr/index.php/LRP_IoT) for additionnal information.


The dataset is available in Zenodo (https://zenodo.org/) 


## Dataset

All the packets are part of an elasticsearch index (`lora-strasbourg-anonymous`) which contains the following format:

```
{
  "mappings": {
    "properties": {
      "devAddr": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "devEui": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "dup_infos": {
        "properties": {
          "copy_of": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "is_duplicate": {
            "type": "boolean"
          },
          "version": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        }
      },
      "extra_infos": {
        "properties": {
          "phyPayload": {
            "properties": {
              "macPayload": {
                "properties": {
                  "appEUI": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "appnonce": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "devEUI": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "devNonce": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "devaddr": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "fPort": {
                    "type": "long"
                  },
                  "fhdr": {
                    "properties": {
                      "devAddr": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "fCnt": {
                        "type": "long"
                      },
                      "fCtrl": {
                        "properties": {
                          "ack": {
                            "type": "boolean"
                          },
                          "adr": {
                            "type": "boolean"
                          },
                          "adrAckReq": {
                            "type": "boolean"
                          },
                          "classB": {
                            "type": "boolean"
                          },
                          "fOptsLen": {
                            "type": "long"
                          },
                          "fPending": {
                            "type": "boolean"
                          }
                        }
                      }
                    }
                  },
                  "netid": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              },
              "mhdr": {
                "properties": {
                  "mType": {
                    "type": "long"
                  },
                  "major": {
                    "type": "long"
                  }
                }
              },
              "mic": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "version": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        }
      },
      "mType": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "rxInfo": {
        "properties": {
          "antenna": {
            "type": "long"
          },
          "board": {
            "type": "long"
          },
          "channel": {
            "type": "long"
          },
          "context": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "crcStatus": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "fineTimestampType": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "gatewayId": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "gatewayId_hash": {
            "type": "float"
          },
          "mac": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "metadata": {
            "properties": {
              "region_common_name": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "region_config_id": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          },
          "rfChain": {
            "type": "long"
          },
          "rssi": {
            "type": "long"
          },
          "snr": {
            "type": "long"
          },
          "time": {
            "type": "date"
          },
          "timeSinceGpsEpoch": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "uplinkId": {
            "type": "long"
          },
          "uplinkIdText": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          }
        }
      },
      "rxInfobis": {
        "properties": {
          "channel": {
            "type": "long"
          },
          "context": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "crcStatus": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "gatewayId": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "rssi": {
            "type": "long"
          },
          "snr": {
            "type": "float"
          },
          "time": {
            "type": "date"
          },
          "uplinkId": {
            "type": "long"
          }
        }
      },
      "src_version": {
        "type": "long"
      },
      "time": {
        "type": "date"
      },
      "txInfo": {
        "properties": {
          "frequency": {
            "type": "long"
          },
          "fskModulationInfo": {
            "properties": {
              "datarate": {
                "type": "long"
              },
              "frequencyDeviation": {
                "type": "long"
              }
            }
          },
          "modulation": {
            "properties": {
              "lora": {
                "properties": {
                  "bandwidth": {
                    "type": "long"
                  },
                  "codeRate": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "polarizationInversion": {
                    "type": "boolean"
                  },
                  "spreadingFactor": {
                    "type": "long"
                  }
                }
              },
              "type": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

`extra_infos`contains the packet's header, decoded from the original `phyPayload` (ack required, devAddr, etc.). It is worth noting we didn't include the original PHY payload for privacy concerns, i.e., only the headers are provided.




## Import / Install

You need first to install Elastic Search in a Virtual Machine / Server. You may look at [https://github.com/ICube-Networks/lora-analysis](https://github.com/ICube-Networks/lora-analysis) which provides scripts to install and configure automatically an elastic search instance (VM or standalone debian-like daemon).

To reimport this snapshot in your elastic serch server, here is the procedure using Kibana:

1. Download the dataset
	- download the dataset from e.g., Zenodo
	- uncompress the dataset locally in your server (e.g., in `'$HOME/dataset'`
1. Add the repository to elastic search
	- add the directive `path.repo:['$HOME/dataset']` in your configuration file (`/etc/elasticsearch/elasticsearch.yml`)  
1. Configure the repository in Kibana
	- select `Management/Stack Management` in Kibana (https://url:5601 by defauylt). 
	- select `Snaphot and Restore`
	- in `Repositories`, register a new repository:
		- `Shared File system`
		- location: the directory you specified in `path.repo`
	- To restore a specific snapshot
		- in the `Snapshots` menu, select a specific snapshot (e.g., `lora-anonymous-weekly-snap-2026.04.18-ujwinqnrsoebeif-fq75tg`)
		- select `restore`
		- the snapshot contains one unique elastic search index (i.e., `lora-strasbourg-anonymous`)

Alternatively, you can look at the elastic search documentation for snapshot and restore ([https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/restore-snapshot](https://www.elastic.co/docs/deploy-manage/tools/snapshot-and-restore/restore-snapshot))

Your elastic search index (`lora-strasbourg-anonymous `) is now ready to be used.


## Analysis

We provide an openaccess to all the scripts we implemented to analyze this dataset : [https://github.com/ICube-Networks/lora-analysis](https://github.com/ICube-Networks/lora-analysis):

- detection of duplicated packets (one packet received by several gateways)
- identification of LoRa flows
- distribution of operators
- distribution of flow's characteristics (duration, inter packet time, etc.)
- etc.
