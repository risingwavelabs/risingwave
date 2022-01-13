import argparse
import os
import json


table_pattern = {
    'supplier': [('s_suppkey', int), ('s_name', str), ('s_address', str),
                 ('s_nationkey', int), ('s_phone', str), ('s_acctbal', float),
                 ('s_comment', str)],
    'part': [('p_partkey', int), ('p_name', str), ('p_mfgr', str),
                 ('p_brand', str), ('p_type', str), ('p_size', int),
                 ('p_container', str), ('p_retailprice', float), ('p_comment', str)],
    'partsupp': [('ps_partkey', int), ('ps_suppkey', int), ('ps_availqty', int),
                 ('ps_supplycost', float), ('ps_comment', str)],
    'customer': [('c_custkey', int), ('c_name', str), ('c_address', str),
                 ('c_nationkey', int), ('c_phone', str), ('c_acctbal', float),
                 ('c_mktsegment', str), ('c_comment', str)],
    'orders': [('o_orderkey', int), ('o_custkey', int), ('o_orderstatus', str),
               ('o_totalprice', float), ('o_orderdate', str), ('o_orderpriority', str),
               ('o_clerk', str), ('o_shippriority', int), ('o_comment', str)],
    'lineitem': [('l_orderkey', int), ('l_partkey', int), ('l_suppkey', int),
                 ('l_linenumber', int), ('l_quantity', float), ('l_extendedprice', float),
                 ('l_discount', float), ('l_tax', float), ('l_returnflag', str),
                 ('l_linestatus', str), ('l_shipdate', str), ('l_commitdate', str),
                 ('l_receiptdate', str), ('l_shipinstruct', str), ('l_shipmode', str),
                 ('l_comment', str)],
    'nation': [('n_nationkey', int), ('n_name', str), ('n_regionkey', int),
               ('n_comment', str)],
    'region': [('r_regionkey', int), ('r_name', str), ('r_comment', str)]
}


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", required=True)
    args = parser.parse_args()
    return args


def parse_line(pattern, line):
    if pattern not in table_pattern.keys():
        os.exit(1)
    mapping = table_pattern[pattern]
    res = dict()
    for value, (name, op) in zip(line.strip().split('|'), mapping):
        res[name] = op(value)
    print(json.dumps(res))


def main():
    args = parser()
    with open(args.file, 'r') as f:
        for line in f.readlines():
            pattern = args.file[:-4]
            parse_line(pattern, line)


if __name__ == "__main__":
    main()
