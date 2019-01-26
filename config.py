#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    configure
"""
from multiprocessing import cpu_count


class Config(object):
    # kafka
    kafka_bootstrap_servers = '127.0.0.1:9092'
    kafka_topic = 'topic-db'
    kafka_group = 'consumer-group-db'

    # sync binlog to db
    db_ip = '127.0.0.1'
    db_port = 3306
    db_username = 'root'
    db_password = '123456'
    db_name = 'test'

    # num of consumer processes
    PROCESS_NUM = cpu_count()


# table -> [pri_keys]
table_pri_dict = {}
