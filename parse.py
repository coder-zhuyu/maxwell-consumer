#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    parse maxwell to sql
"""
import json
import logging
from config import Config
import traceback
from config import table_pri_dict


class MaxwellParse(object):
    def __init__(self, data):
        self.data = data
        self.data_dict = {}
        self.database = ''
        self.table = ''
        self.pri_keys = []
        self.sql_type = ''

    def _gen_insert_sql(self):
        sql = ''
        columns = []
        values = []
        for k, v in self.data_dict['data'].items():
            columns.append('`' + k + '`')
            if v is not None:
                if type(v) in (str, unicode):
                    values.append("'" + v + "'")
                else:
                    values.append(str(v))
            else:
                values.append('null')
        if not columns:
            return sql
        sql += 'replace into' + ' `' + self.table + '` ' + \
               '(' + ', '.join(columns) + ')' + \
               ' values ' + '(' + ', '.join(values) + ')'
        return sql

    def _gen_update_sql(self):
        sql = 'update ' + '`' + self.table + '`' + ' set '

        kvs = []
        for k, v in self.data_dict['data'].items():
            if v is None:
                kvs.append('`' + k + '`' + '=' + 'null')
            elif type(v) in (str, unicode):
                kvs.append('`' + k + '`' + '=' + "'" + v + "'")
            else:
                kvs.append('`' + k + '`' + '=' + str(v))
        sql += ', '.join(kvs) + ' where '

        kvs = []
        for k in self.pri_keys:
            if k in self.data_dict['old']:
                v = self.data_dict['old'][k]
            else:
                v = self.data_dict['data'][k]
            if type(v) in (str, unicode):
                kvs.append('`' + k + '`' + '=' + "'" + v + "'")
            else:
                kvs.append('`' + k + '`' + '=' + str(v))

        sql += ' and '.join(kvs)

        return sql

    def _gen_delete_sql(self):
        sql = 'delete from ' + '`' + self.table + '`' + ' where '
        kvs = []
        for k in self.pri_keys:
            v = self.data_dict['data'][k]
            if type(v) in (str, unicode):
                kvs.append('`' + k + '`' + '=' + "'" + v + "'")
            else:
                kvs.append('`' + k + '`' + '=' + str(v))

        sql += ' and '.join(kvs)

        return sql

    def _trans(self):
        if 'data' in self.data_dict:
            for k, v in self.data_dict['data'].items():
                if v == '1970-01-01 00:00:00' or v == '1970-01-01 08:00:00':
                    self.data_dict['data'][k] = '0000-00-00 00:00:00'
                    continue
                if type(v) in (str, unicode):
                    v = v.replace("'", r"\'")
                    self.data_dict['data'][k] = v

        if 'old' in self.data_dict:
            for k, v in self.data_dict['old'].items():
                if v == '1970-01-01 00:00:00' or v == '1970-01-01 08:00:00':
                    self.data_dict['old'][k] = '0000-00-00 00:00:00'
                    continue
                if type(v) in (str, unicode):
                    v = v.replace("'", r"\'")
                    self.data_dict['old'][k] = v

    def gen_sql(self):
        try:
            self.data_dict = json.loads(self.data)
            if type(self.data_dict) is not dict:
                logging.error('not json')
                return ''

            self.database = self.data_dict.get('database', '')
            self.table = self.data_dict.get('table', '')
            self.pri_keys = table_pri_dict.get(self.table, [])
            self.sql_type = self.data_dict.get('type', '').lower()
            if not self.pri_keys and self.sql_type not in ('table-alter', 'table-create'):
                logging.critical('no primary key')
                return ''

            self._trans()

            if self.sql_type == 'insert':
                sql = self._gen_insert_sql()
            elif self.sql_type == 'update':
                sql = self._gen_update_sql()
            elif self.sql_type == 'delete':
                sql = self._gen_delete_sql()
            else:
                sql = self.data_dict.get('sql', '')
                if self.database:
                    sql = sql.replace('`' + self.database + '`', '`' + Config.db_name + '`').replace('\r\n', '')
        except Exception as e:
            logging.error(repr(e))
            logging.error(traceback.format_exc())
            return ''
        return sql
