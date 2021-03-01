# jSQLon: json and SQLite together at last

jsqlon is a simple interface to back up/restore SQLite to/from json.
This enables storing a database in a version control system like `git`.

# Installation

```bash
git clone https://github.com/cbosoft/jsqlon
cd jsqlon && pip install .
```

# Use

```python
from jsqlon import Database

with Database('main.db') as db:
    results = db.query('SELECT * FROM Table2;')

for result in results:
    print(result['ID'])
```

```python
from jsqlon import Database

def a_custom_factory(cursor, row):
    """Simple example of factory."""
    names = [d[0] for d in cursor.description]
    return {n:v for n, v in zip(names, row)}

if __name__ == '__main__':
    with Database('main.db') as db:
        results = db.query('SELECT * FROM Table2;', factory=a_custom_factory)
    
    for result in results:
        print(result[0])
```