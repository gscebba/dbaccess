#  Copyright (c) 2022. Gaetano Scebba
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
#  documentation files (the "Software"), to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
#  and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all copies or substantial portions
#  of the Software.
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
#  TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
#  CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.

import sqlite3
import sys
import traceback
from pathlib import Path
from typing import List

import pandas as pd


class DbAccess(object):
    def __init__(self, data_dir: Path, db_name: str):
        """
        :param data_dir: Path to the .db file
        :param db_name:  database file name
        """
        if isinstance(data_dir, str):
            data_dir = Path(data_dir)
        self.db = self._connect(data_dir, db_name)

    @staticmethod
    def _connect(data_dir: Path, db_name: str) -> sqlite3.Connection:
        db = sqlite3.connect(str(data_dir / db_name))
        db.execute("PRAGMA journal_mode = OFF;")
        db.execute("PRAGMA page_size = 16384;")
        return db

    def _pre(self, query: str) -> str:
        return self._append_semicolumn(query), self.db.cursor()

    def read(self, query: str) -> List:
        query, cur = self._pre(query)
        data = cur.execute(query).fetchall()
        cur.close()
        return data

    def read_one(self, query: str) -> List:
        query, cur = self._pre(query)
        data = cur.execute(query).fetchone()
        cur.close()
        return data

    def write(self, query: str) -> None:
        query, cur = self._pre(query)
        try:
            cur.execute(query)
            self.db.commit()
        except sqlite3.Error as er:
            print(f'SQLite error: {er.args}')
            print(f'Exception class: {er.__class__}')
            print('SQLite traceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
            print(query)
        finally:
            if self.db:
                cur.close()

    def write_many(self, query: str, values: List) -> None:
        '''
        # The qmark style used with executemany():
        lang_list = [
            ("Fortran", 1957),
            ("Python", 1991),
            ("Go", 2009),
        ]
        cur.executemany("insert into lang values (?, ?)", lang_list)
        :param values:
        :param query:
        :return:
        '''
        query, cur = self._pre(query)
        try:
            cur.executemany(query, values)
            self.db.commit()
        except sqlite3.Error as er:
            print(f'SQLite error: {er.args}')
            print(f'Exception class: {er.__class__}')
            print('SQLite atraceback: ')
            exc_type, exc_value, exc_tb = sys.exc_info()
            print(traceback.format_exception(exc_type, exc_value, exc_tb))
        finally:
            if self.db:
                cur.close()

    def _check_structure(self) -> None:
        tables = pd.read_sql_query('SELECT * FROM sqlite_master ;', self.db)
        print(tables)

    def _check_table(self, table_name: str) -> None:
        table = pd.read_sql_query(f'SELECT * FROM {table_name};', self.db)
        print(table)

    @staticmethod
    def _append_semicolumn(query: str) -> str:
        if query[-1] != ';':
            query += ';'
        return query
