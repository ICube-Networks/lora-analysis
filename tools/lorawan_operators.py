# -*- coding:utf-8 -*-

""" LoRaWAN Operators """

import csv, os

def load_operators_csv():
    """ Load operators file """
     
    operators = []
    with open(os.path.dirname(__file__)+'/operators_prefix.csv') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row in reader:
            #print row
            #print(row['netid'], row['name'])
            operators.append(row)
    return operators

def check_devaddr_operator(devaddr, range):
    """ Check if devAddr is in a given operator prefix"""
    _range = range.split(" ")
    # hex numbers to int
    _begin = int(_range[0], 16)
    _end = int(_range[2], 16)
    _devaddr = int(devaddr, 16)

    #print _begin, _devaddr, _end
    if _begin <= _devaddr <= _end:
        return True
    else:
        return False

def find_operators(operators, devaddr):
    """ Find operator from devaddr """
    op_name = "UNKOWN"
    for operator in operators:
        if check_devaddr_operator(devaddr, operator['range']) is True:
            op_name = operator['name']
    return op_name

if __name__ == '__main__':
    OPERATORS = load_operators_csv()
    #print OPERATORS
    print(find_operators(OPERATORS, "1ddec524"))
