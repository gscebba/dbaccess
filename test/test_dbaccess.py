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

import os
import sqlite3
import pytest
from src.dbaccess import DbAccess


@pytest.fixture
def my_db():
    connection = sqlite3.connect('./testdb.db')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS table_one (id INTEGER PRIMARY KEY, val TEXT);')
    connection.commit()
    cursor.execute('CREATE TABLE IF NOT EXISTS table_two (id INTEGER PRIMARY KEY, val TEXT);')
    connection.commit()
    cursor.execute('INSERT INTO table_two (id, val) VALUES (0, "z")')
    connection.commit()
    cursor.execute('INSERT INTO table_two (id, val) VALUES (1, "x")')
    connection.commit()
    cursor.close()
    yield connection
    connection.close()
    os.remove('./testdb.db')

def test_read(my_db):
    db = DbAccess('./', 'testdb.db')
    info = db.read('PRAGMA table_info("table_one")')
    assert info[1][1] == 'val'
    data = db.read('SELECT * FROM table_two')
    assert data[1][1] == 'x'


def test_read_one(my_db):
    db = DbAccess('./', 'testdb.db')
    data = db.read_one('SELECT * FROM table_two')
    assert data[1] == 'z'


def test_write(my_db):
    db = DbAccess('./', 'testdb.db')
    db.write('INSERT INTO table_one (id, val) VALUES (1, "a")')

    cursor = my_db.cursor()
    data = cursor.execute('SELECT * FROM table_one').fetchall()
    cursor.close()
    assert data[0][1] == 'a'


def test_write_many(my_db):
    db = DbAccess('./', 'testdb.db')
    data_list = [
        (1, 'a'),
        (2, 'b'),
        (3, 'c'),
        (4, 'd')
    ]
    db.write_many('INSERT INTO table_one (id, val) VALUES (?,?)', data_list)

    cursor = my_db.cursor()
    data = cursor.execute('SELECT * FROM table_one').fetchall()
    cursor.close()
    assert data[2][1] == 'c'


def test__append_semicolumn():
    query = 'SELECT * FROM table'
    query = DbAccess._append_semicolumn(query)
    query_with = query + ';'
    assert query[-1] == ';'
    assert query_with[-1] == ';'
