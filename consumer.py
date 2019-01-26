#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    consumer kafka
"""
# import sys
import logging
import pymysql
from confluent_kafka import Consumer, KafkaError
from graceful import GracefulExitException
from parse import MaxwellParse
import traceback


class KafkaConsumer(object):
    def __init__(self, config):
        self.consumer = Consumer({
            'bootstrap.servers': config['bootstrap_servers'],
            'group.id': config['group_id'],
            'default.topic.config': {
                'auto.offset.reset': 'smallest'
            },
            'enable.auto.commit': 'false'
        })
        self.db_conn = pymysql.connect(
            host=config['ip'],
            port=config['port'],
            user=config['username'],
            password=config['password'],
            db=config['dbname'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    def _output_db(self, sql):
        with self.db_conn.cursor() as cursor:
            try:
                cursor.execute(sql)
                self.db_conn.commit()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                self.db_conn.rollback()
                raise GracefulExitException()
                # sys.exit(1)

    def subscribe(self, topic_list):
        self.consumer.subscribe(topic_list)
        # self.topic = topic_list[0]

    def process(self):
        msg = self.consumer.poll(timeout=2)
        if msg is None:
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                logging.error(msg.error())
            return
        data = msg.value().decode('utf-8')
        logging.info(u'Received message: {}'.format(data))
        logging.info('partition:{} offset:{}'.format(msg.partition(), msg.offset()))
        sql_parse = MaxwellParse(data)
        sql = sql_parse.gen_sql()
        if sql:
            logging.info(u'SQL: {}'.format(sql))
            self._output_db(sql)
        logging.info('offset commit partition:{} offset:{}'.format(msg.partition(), msg.offset()))
        self.consumer.commit()

        if sql_parse.sql_type in ('table-alter', 'table-create'):
            from run import gen_table_pri_dict
            gen_table_pri_dict()

        return

    def close(self):
        logging.info("--------closing---------")
        if self.consumer:
            self.consumer.close()
