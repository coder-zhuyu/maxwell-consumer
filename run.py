#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    run
"""
import os
import multiprocessing
import sys
from consumer import KafkaConsumer
from config import Config, table_pri_dict
import logging
import logger
import traceback
import pymysql
from graceful import GracefulExitEvent, GracefulExitException


def gen_table_pri_dict():
    db_conn = pymysql.connect(
        host=Config.db_ip,
        port=Config.db_port,
        user=Config.db_username,
        password=Config.db_password,
        db='information_schema',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    with db_conn.cursor() as cursor:
        sql = '''
            SELECT table_name, GROUP_CONCAT(column_name) AS column_names FROM information_schema.`COLUMNS` 
            WHERE table_schema=%s AND column_key='pri'
            GROUP BY table_name
        '''
        cursor.execute(sql, (Config.db_name,))
        for row_dict in cursor.fetchall():
            table_pri_dict[row_dict['table_name']] = row_dict['column_names'].split(',')

    db_conn.close()
    logging.info(table_pri_dict)


def worker_proc(gee):
    logging.info("worker({}) start ...".format(os.getpid()))
    kafka_consumer = None
    try:
        gen_table_pri_dict()

        kafka_db_config = {
            'bootstrap_servers': Config.kafka_bootstrap_servers,
            'group_id': Config.kafka_group,
            'ip': Config.db_ip,
            'port': Config.db_port,
            'username': Config.db_username,
            'password': Config.db_password,
            'dbname': Config.db_name
        }
        kafka_consumer = KafkaConsumer(kafka_db_config)
        kafka_consumer.subscribe([Config.kafka_topic])
        while not gee.is_stop():
            kafka_consumer.process()
    except GracefulExitException:
        logging.info("worker({}) got GracefulExitException".format(os.getpid()))
    except Exception, ex:
        logging.error("Exception:{}".format(ex))
        logging.error(traceback.format_exc())
    finally:
        if kafka_consumer:
            kafka_consumer.close()
        logging.info("worker({}) exit.".format(os.getpid()))
        sys.exit(0)


if __name__ == "__main__":
    logger.init()
    logging.info("main process({}) start".format(os.getpid()))

    gee = GracefulExitEvent()

    # Start some workers process and run forever
    for i in range(Config.PROCESS_NUM):
        wp = multiprocessing.Process(target=worker_proc, args=(gee,))
        wp.start()
        gee.reg_worker(wp)

    gee.wait_all()
    sys.exit(0)
