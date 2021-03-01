import os
import sqlite3
from contextlib import closing
from shutil import copyfile
from datetime import datetime
import json
import re
import hashlib

from typing import List, Tuple

from .exceptions import JsqlonBaseError


class DatabaseError(JsqlonBaseError):
    '''Raised when something goes wrong when accessing the database.'''


class NoResults(DatabaseError):
    '''Raised when no results are returned.'''


class Database:

    def __init__(self, path: str = '../data/data.db', dummy=False):
        self.path = path
        self.dummy = dummy
        self.recovered = False

    def __enter__(self):
        self.maybe_load_backup()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.maybe_save_backup()

    def __getitem__(self, item: [str, Tuple[str, callable]]):
        if isinstance(item, tuple):
            return self.query(*item)
        elif isinstance(item, str):
            return self.query(item)
        else:
            raise ValueError(f'Query must be str or tuple of str and callable.')

    @property
    def backup_path(self):
        return self.path+'.json'

    def load_backup(self):
        if os.path.exists(self.path):
            today = datetime.today().strftime('%Y-%m-%dT%H%M')
            bkp_db = f'{self.path}.{today}.bak'
            copyfile(self.path, bkp_db)
            os.remove(self.path)

        with open(self.backup_path) as f:
            backup_data = json.load(f)

        # backup data is dictionary with tablename keys
        # each table value is a dict with row data and column spec
        create_statements = [self.create_statement_from_data(name=k, **v) for k,v in backup_data.items()]

        # first off, need to create the tables
        self.execute_sql(create_statements)

        # with all the tables in place, now to put data into tables
        populate_statements = [self.populate_from_data(name=k, **v) for k, v in backup_data.items()]
        for statements in populate_statements:
            self.execute_sql(statements)

        print(f'Recovered SQLite database from text backup "{self.backup_path}".')
        self.recovered = True

    def maybe_load_backup(self):
        if not os.path.exists(self.backup_path):
            # no backup to load from
            return

        if not os.path.exists(self.path):
            # backup is sole copy
            print('SQLite database does not yet exist.')
            self.load_backup()
        else:
            if self.backup_is_newer():
                print('JSON backup is newer than SQLite database.')
                self.load_backup()

    def as_storable_dict(self) -> dict:
        data = dict()
        # read from database all table schema
        sources = self.query(
            'SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE "sqlite_%";',
            factory=lambda c, t: t[0])
        create_re = re.compile(r'^CREATE TABLE (\w+) \((.*)\);?$')
        for sql in sources:
            sql = ' '.join(sql.split('\n'))
            if m := create_re.match(sql):
                name, column_data = m.groups()
                column_data = [c.strip() for c in column_data.split(',')]
                columns = dict()
                for cdata in column_data:
                    n, t = cdata.split()[:2]
                    c = dict(datatype=t)
                    if 'NOT NULL' in cdata:
                        c['not_null'] = True
                    if 'UNIQUE' in cdata:
                        c['unique'] = True
                    if 'AUTOINCREMENT' in cdata:
                        c['autoincrement'] = True
                    if 'PRIMARY KEY' in cdata:
                        c['primary_key'] = True
                    columns[n] = c
                data[name] = dict(rows=list(), columns=columns)
            else:
                raise Exception(f'Malformed SQL from db: {sql}')

        # read from each table the values of all rows
        for table, tabledata in data.items():
            rows = self.query(f'SELECT * FROM {table};', factory=sqlite3.Row)
            rows = [dict(row) for row in rows]
            data[table]['rows'] = rows

        return data

    def save_backup(self):
        data = self.as_storable_dict()
        print(f'Writing text backup for database "{self.path}".')
        with open(self.backup_path, 'w') as f:
            json.dump(data, f, indent=2)
            f.write('\n')

    def maybe_save_backup(self):
        if self.recovered:
            # don't save backup if has just read from backup
            return

        if not os.path.exists(self.backup_path):
            # no backup exists
            print('JSON backup does not yet exist.')
            self.save_backup()
        else:
            if self.backup_is_older():
                # sqlitedb is newer than json backup
                print('JSON backup is older than SQLite database')
                self.save_backup()

    def query(self, query: str, factory=sqlite3.Row):
        assert isinstance(query, (str, list))

        with closing(sqlite3.connect(self.path)) as conn:
            conn.row_factory = factory
            with conn:
                with closing(conn.cursor()) as cur:
                    cur.execute(query)
                    results = cur.fetchall()
        if not results:
            raise NoResults(f'No results returned for query: "{query}".')
        return results

    def execute_sql(self, command: [str, List[str]]):
        if self.dummy:
            print(command)
            return
        with closing(sqlite3.connect(self.path)) as conn:
            with conn:
                with closing(conn.cursor()) as cur:
                    if isinstance(command, str):
                        cur.execute(command)
                    else:
                        cur.execute('BEGIN TRANSACTION;')
                        for cmnd in command:
                            try:
                                cur.execute(cmnd)
                            except sqlite3.OperationalError:
                                print(cmnd)
                                raise
                        cur.execute('END TRANSACTION;')

    def create_statement_from_data(self, *, name: str, columns: dict, **kwargs) -> str:
        cols = [self.column_spec_from_data(name=n, **spec) for n, spec in columns.items()]
        cols = ', '.join(cols)
        return f'CREATE TABLE {name} ({cols});'

    def populate_from_data(self, *, name: str, rows: List[dict], **kwargs) -> List[str]:
        return [self.insert_statement_from_data(name, row) for row in rows]

    @staticmethod
    def column_spec_from_data(*, name: str, datatype: dict, default=None, not_null=False, unique=False,
                              primary_key=False, autoincrement=False) -> str:
        s = f'{name} {datatype}'
        if default is not None:
            s += f' DEFAULT {default}'
        if not_null:
            s += ' NOT NULL'
        if unique:
            s += ' UNIQUE'
        if primary_key:
            s += ' PRIMARY KEY'
            if autoincrement:
                s += ' AUTOINCREMENT'
        return s

    @staticmethod
    def insert_statement_from_data(name: str, data: dict) -> str:
        data = dict(**data)
        for k in list(data.keys()):
            if data[k] is None:
                del data[k]
        cols = ','.join(data.keys())
        vals = list()
        for val in data.values():
            if isinstance(val, (int, float)):
                vals.append(str(val))
            elif isinstance(val, str):
                val = val.replace('"', '""')
                vals.append(f'"{val}"')
            else:
                raise ValueError(f'unknown type inserted into db. {type(val)}: {val}')
        vals = ', '.join(vals)
        return f'INSERT INTO {name} ({cols}) VALUES ({vals});'

    @staticmethod
    def hash_of_storable(data: dict):
        return hash(str(data))

    def hash(self):
        return self.hash_of_storable(self.as_storable_dict())

    def backed_up_hash(self):
        with open(self.backup_path) as f:
            data = json.load(f)
        return self.hash_of_storable(data)

    def backup_is_same(self) -> bool:
        return self.hash() == self.backed_up_hash()

    def backup_mtime(self):
        return os.path.getmtime(self.backup_path)

    def mtime(self):
        return os.path.getmtime(self.path)

    def backup_is_newer(self):
        if self.backup_is_same():
            return False
        return self.backup_mtime() > self.mtime()

    def backup_is_older(self):
        if self.backup_is_newer():
            return False
        return self.backup_mtime() < self.mtime()
