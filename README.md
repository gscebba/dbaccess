# SQLite Helper

[![Build Status](https://app.travis-ci.com/gscebba/dbaccess.svg?branch=main)](https://app.travis-ci.com/gscebba/dbaccess)
[![codecov](https://codecov.io/gh/gscebba/dbaccess/branch/main/graph/badge.svg?token=5EMF22QP2U)](https://codecov.io/gh/gscebba/dbaccess)

Python class helper to work with sqlite database.


### Usage
```python
from dbaccess import DbAccess
data_list = [
    (1, 'a'),
    (2, 'b'),
    (3, 'c'),
    (4, 'c')
]

db = DbAccess('./', 'mydb.db')
db.write('CREATE TABLE IF NOT EXISTS table_one ('
         'id INTEGER PRIMARY KEY,'
         'val TEXT)')

db.write(f'INSERT INTO table_one (id, val) VALUES {data_list[0]}')
db.write('CREATE TABLE IF NOT EXISTS table_two ('
         'id INTEGER PRIMARY KEY,'
         'val TEXT)')
data = db.read_one('SELECT * FROM table_one')

db.write_many('INSERT INTO table_two (id, val) VALUES (?,?)', data_list)
data = db.read('SELECT * FROM table_two')
```

### License
Distributed under the MIT License. See `LICENSE.txt` for more information.

### Contact
Gaetano Scebba - [@GScebba](https://twitter.com/GScebba)




