#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    logging
"""
import os
import logging
from logging import Formatter
from logging.handlers import RotatingFileHandler


def init(log_path=''):
    if not log_path:
        log_path = "./maxwell-consumer.log"

    file_handler = RotatingFileHandler(log_path,
                                       mode='w', maxBytes=1024 * 1024 * 500,
                                       backupCount=10,
                                       encoding='utf-8')

    file_handler.setFormatter(Formatter(
        '%(asctime)s [%(process)d] [in %(pathname)s:%(lineno)d] %(levelname)s: %(message)s '
    ))

    if os.getenv('ENV') != 'production':
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger().setLevel(logging.INFO)

    logging.getLogger().addHandler(file_handler)
