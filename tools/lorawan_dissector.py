# -*- coding:utf-8 -*-

""" LoRaWAN Gateway RX message dissector """

import base64
import binascii
import json
import logging
import struct
import sys

import pprint

#hash
import hashlib

#parameters
EXTRA_INFO_VERSION = "1.1"


MTYPE_JOIN_REQUEST = 0
MTYPE_JOIN_ACCEPT = 1
MTYPE_UNCONFIRMED_DATA_UP = 2
MTYPE_UNCONFIRMED_DATA_DOWN = 3
MTYPE_CONFIRMED_DATA_UP = 4
MTYPE_CONFIRMED_DATA_DOWN = 5
MTYPE_RFU = 6
MTYPE_PROPRIETARY = 7

LOGGER = logging.getLogger('phypayload_dissector')
logging.basicConfig(stream=sys.stdout, level=logging.INFO)

def get_mtype(byte):
    """ return MType from MHDR """
    return struct.unpack("b", byte)[0] >> 5

def process_phypayload(phypayload):
    """ process LoRaWAN Gateway PHYPayload """
    extra_infos = {}
    extra_infos['version'] = EXTRA_INFO_VERSION
    extra_infos['phyPayload'] = {}

    if phypayload == "":
        LOGGER.info("INVALID Length")
        return extra_infos

    bin_data = {}
    bin_data['phypayload'] = base64.b64decode(phypayload)
    bin_data['macPayload'] = bin_data['phypayload'][1:-4]
    bin_data['mhdr'] = bin_data['phypayload'][0]
    bin_data['mic'] = bin_data['phypayload'][-4:]

    if len(bin_data['phypayload']) == 1 + len(bin_data['macPayload']) + len(bin_data['mic']):
        pass
    else:
        LOGGER.info("INVALID Length")
        return extra_infos

    phypayload = bin_data['phypayload']


    #extra_infos['phyPayload']['bytes'] = binascii.hexlify(bin_data['phypayload'])
    extra_infos['phyPayload'].update(decode_mhdr(bin_data['mhdr']))
    extra_infos['phyPayload']['macPayload'] = {}
    extra_infos['phyPayload']['mic'] = binascii.hexlify(bin_data['mic']).decode()  #json compliant
    extra_infos['phyPayload']['length'] = len(bin_data['phypayload'])
 
    mtype = extra_infos['phyPayload']['mhdr']['mType']

    if mtype == MTYPE_JOIN_ACCEPT:
        extra_infos['phyPayload'].update(decode_join_accept(bin_data))
    elif mtype == MTYPE_JOIN_REQUEST:
        extra_infos['phyPayload'].update(decode_join_request(bin_data))
    elif mtype == MTYPE_UNCONFIRMED_DATA_UP:
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    elif mtype == MTYPE_UNCONFIRMED_DATA_DOWN:
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    elif mtype == MTYPE_CONFIRMED_DATA_UP:
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    elif mtype == MTYPE_CONFIRMED_DATA_DOWN:
        #LOGGER.info("** Downlink phypayload: %s", binascii.hexlify(bin_data['phypayload']))
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    elif mtype == MTYPE_RFU:
        #decode_data_genericLOGGER.info("** RFU phypayload: %s", binascii.hexlify(bin_data['phypayload']))
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    elif mtype == MTYPE_PROPRIETARY:
        #LOGGER.info("** Proprietary phypayload: %s", binascii.hexlify(bin_data['phypayload']))
        extra_infos['phyPayload'].update(decode_data_generic(bin_data, mtype))
    else:
         LOGGER.info("*** Unsupported type: %d  (payload = %s)", mtype, binascii.hexlify(bin_data['phypayload']))

    #random field (hash of the payload) when we have to select randomly packets
    extra_infos['random'] = hashlib.sha3_256(bin_data['phypayload']).hexdigest()
   
    #display_extra_infos(bin_data, extra_infos)
    return extra_infos


def display_extra_infos(bin_data, extra_infos):
    """ Display parsed info """
    #LOGGER.debug("phypayload: %s", bin_data['phypayload'])
    print_data_type(extra_infos['phyPayload']['mhdr']['mType'])
    #if extra_infos['phyPayload']['mhdr']['mType'] == 4:
    pp = pprint.PrettyPrinter(indent=4)
    #pp.pprint(extra_infos)
    my_data = json.dumps(extra_infos, sort_keys=True, indent=4)
    print(my_data)

def decode_mhdr(bin_data):
    """
        Decode MAC Header (MHDR field)

        Bit#      7..5  4..2  1..0
        MHDR bits MType RFU   Major
    """
    
    output = {}
    output['mhdr'] = {}
    #print bin(int(bin_data))
    mhdr = bin_data
    output['mhdr']['mType'] = mhdr >> 5
    output['mhdr']['major'] = mhdr & 0b00000011
    return output


def decode_join_accept(bin_data):
    """ Decode Join-Accept """
    phypayload = bin_data['phypayload']
    output = {}
    output['macPayload'] = {}
    mhdr = phypayload[0]
    macpayload = phypayload[1:-4]
    mic = phypayload[-4:]
    assert (1 + len(macpayload) + len(mic)) == len(phypayload)

    # Data encrypted ...
    appnonce = macpayload[0:3]
    netid = macpayload[3:6]
    devaddr = macpayload[6:10]
    output['macPayload']['appnonce'] = binascii.hexlify(appnonce).decode() #json compliant
    output['macPayload']['netid'] =    binascii.hexlify(netid).decode() #json compliant
    output['macPayload']['devaddr'] =  binascii.hexlify(devaddr).decode() #json compliant
    return output

def decode_join_request(bin_data):
    """ Decode Join-Request """
    phypayload = bin_data['phypayload']
    output = {}
    output['macPayload'] = {}
    if len(phypayload) == 23:
        pass
    else:
        return output

    appeui = phypayload[1:9]
    deveui = phypayload[9:17]
    nonce = phypayload[17:19]

    appeui = appeui[::-1]
    deveui = deveui[::-1]
    nonce = nonce[::-1]
    
    output['macPayload']['appEUI'] =   binascii.hexlify(appeui).decode() #json compliant
    output['macPayload']['devEUI'] =   binascii.hexlify(deveui).decode() #json compliant
    output['macPayload']['devNonce'] = binascii.hexlify(nonce).decode()  #json compliant
    return output

def decode_data_generic(bin_data, mtype):
    """ Decode Data """
    output = {}
    output['macPayload'] = {}
    output['macPayload']['fhdr'] = {}
    macpayload = bin_data['macPayload']

    try:
        devaddr, bin_fctrl, fcnt = struct.unpack("=IBH", macpayload[0:7])
    except struct.error as err:
        LOGGER.info("decode_data_generic: Error decoding macPayload: %s", base64.b64encode(bin_data['phypayload']))
        return output
    output['macPayload']['fhdr']['devAddr'] = "%08x" % devaddr
    output['macPayload']['fhdr']['fCnt'] = fcnt

    fctrl = decode_fctrl(bin_fctrl, mtype)
    output['macPayload']['fhdr']['fCtrl'] = fctrl
    #output['macPayload']['fhdr']['fOpts'] = ""

    frmpayload = macpayload[6 + fctrl['fOptsLen'] + 1 :]
    if len(frmpayload) > 0:
        fport = struct.unpack("B", frmpayload[0:1])[0]
    else:
        fport = None
    #if fctrl['fOptsLen'] > 0:
    #    output['macPayload']['fhdr']['fOpts'] = binascii.hexlify(macpayload[7: 7 + fctrl['fOptsLen']])
    #output['macPayload']['fhdr']['bytes'] = binascii.hexlify(macpayload[0 : 7 + fctrl['fOptsLen']])
    output['macPayload']['fPort'] = fport
    #output['macPayload']['frmPayload'] = binascii.hexlify(frmpayload)
    return output

def decode_fctrl(data, mtype):
    """
        Decode FCtrl bits for Uplink
        7   6         5   4       3..0
        ADR ADRACKReq ACK Class B FOptsLen
    """
    #print(data)
    fctrl = {}
    fctrl['adr'] = False
    fctrl['adrAckReq'] = False
    fctrl['ack'] = False
    if mtype == MTYPE_UNCONFIRMED_DATA_UP or mtype == MTYPE_CONFIRMED_DATA_UP:
        fctrl['classB'] = False
    else:
        fctrl['fPending'] = False
    fctrl['fOptsLen'] = 0

    if data & 0b10000000:
        fctrl['adr'] = True
    if data & 0b01000000:
        fctrl['adrAckReq'] = True
    if data & 0b00100000:
        fctrl['ack'] = True
    if data & 0b00010000:
        if mtype == MTYPE_UNCONFIRMED_DATA_UP or mtype == MTYPE_CONFIRMED_DATA_UP:
            fctrl['classB'] = True
        else:
            fctrl['fPending'] = True
    fctrl['fOptsLen'] = data & 0b00001111

    return fctrl


def print_data_type(mtype):
    """ Print message data type """
    if mtype == MTYPE_JOIN_REQUEST:
        LOGGER.debug(" *** Join-request")
    elif mtype == MTYPE_JOIN_ACCEPT:
        LOGGER.debug(" *** Join-Accept")
    elif mtype == MTYPE_UNCONFIRMED_DATA_UP:
        LOGGER.debug(" *** Unconfirmed Uplink")
    elif mtype == MTYPE_UNCONFIRMED_DATA_DOWN:
        LOGGER.debug(" *** Unconfirmed Downlink")
    elif mtype == MTYPE_CONFIRMED_DATA_UP:
        LOGGER.debug(" *** Confirmed Uplink")
    elif mtype == MTYPE_CONFIRMED_DATA_DOWN:
        LOGGER.debug(" *** Confirmed Downlink")


"""

QNFdlwOAtDgB69p4RpQ55y9xNj/4poGNVsImkBhdaQwEW3k=

mType:"UnconfirmedDataUp"
major:"LoRaWANR1"
▶
macPayload:{} 3 keys
▶
fhdr:{} 4 keys
devAddr:"03975dd1"
▶
fCtrl:{} 5 keys
adr:true
adrAckReq:false
ack:false
fPending:false
classB:false
fCnt:14516
fOpts:null
fPort:1
▶
frmPayload:[] 1 item
▶
0:{} 1 key
bytes:"69p4RpQ55y9xNj/4poGNVsImkBhdaQ=="
mic:"0c045b79"
"""

def main():
    """ Main"""
    ## JR zigduino-7
    #process_phypayload("AFR2mJB4VjQSBwAAkHhWNBJpSqd98Ag=")
    # lopy4-05 unconfirmed uplink
    #process_phypayload("QNFdlwOAtDgB69p4RpQ55y9xNj/4poGNVsImkBhdaQwEW3k=")
    # Downlink bug
    #process_phypayload("YBKYPgCFJ5cDQAcAAfKHr5E=")
    # confirmed up
    #process_phypayload("gMEP6QYAW7wB2JZAV68cvyB8xNkBo2GskWM=")
    
    print(process_phypayload('y6PLK1UIAAA='))

if __name__ == "__main__":
    main()


#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'wfnUBXAC7Mw='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'wfnUBXAC7Mw='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'wfnUBXAC7Mw='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'wfnUBXAC7Mw='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'16LBMVUIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'16LBMVUIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'16LBMVUIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxH+JGotRaLPBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxH+JGotRaLPBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxGkEGstRZ/PBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxGkEGstRZ/PBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxH+JGgtRZnPBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxH+JGgtRZnPBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxGkEGgtRZ7PBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxGkEGgtRZ7PBRE='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'WYt430QIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'WYt430QIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxEoC4D8Q3qGBRA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'RxEoC4D8Q3qGBRA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'u6PLK1UIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'u6PLK1UIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'y6PLK1UIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'y6PLK1UIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'y6PLK1UIAAA='
#INFO:phypayload_dissector:decode_data_generic: Error decoding macPayload: b'y6PLK1UIAAA='
