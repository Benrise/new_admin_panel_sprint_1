from enum import Enum
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor


class TablesNamesEnum(Enum):
    FILM_WORK = 'film_work'
    GENRE = 'genre'
    GENRE_FILM_WORK = 'genre_film_work'
    PERSON = 'person'
    PERSON_FILM_WORK = 'person_film_work'


def compare_dbs(connection: sqlite3.Connection, pg_conn: _connection):
    msqlite_tables = get_msqlite_tables(connection)
    postgres_tables = get_postgres_tables(pg_conn)

    assert len(msqlite_tables) == len(postgres_tables), "[Test 1] Unequal number of tables"
    assert is_equal_fields_count(connection,
                                 pg_conn,
                                 msqlite_tables,
                                 postgres_tables
                                 ), "[Test 2] Unequal fields count in tables"
    

    print("Everything passed")


def get_msqlite_tables(conn: sqlite3.Connection):
    query = "SELECT * FROM sqlite_master WHERE type='table';"
    curs = conn.cursor()
    curs.execute(query)
    data = curs.fetchall()
    tables = [table[1] for table in data]
    return tables


def get_postgres_tables(conn: _connection):
    query = """SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'content';"""
    curs = conn.cursor()
    curs.execute(query)
    all_tables = curs.fetchall()
    target_tables = [target_table[0] for target_table in all_tables
                     if target_table[0] in [movie_table.value for movie_table
                                            in TablesNamesEnum]]
    return target_tables


def is_equal_fields_count(sqlite_conn: sqlite3.Connection,
                          postgres_conn: _connection,
                          msqlite_tables: list[str],
                          postgres_tables: list[str]):
    msqlites_tables_fields = []
    postgres_tables_fields = []

    for table in msqlite_tables:
        query = f"SELECT * FROM {table}"
        curs = sqlite_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
        msqlites_tables_fields.append({'table_name': table, 'fields': [len(data)]})

    for table in postgres_tables:
        query = f"SELECT * FROM content.{table}"
        curs = postgres_conn.cursor()
        curs.execute(query)
        data = curs.fetchall()
        postgres_tables_fields.append({'table_name': table, 'fields': [len(data)]})

    are_equal = all(item in postgres_tables_fields for item in msqlites_tables_fields) and all(item in msqlites_tables_fields for item in postgres_tables_fields)
    return are_equal


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5433}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        compare_dbs(sqlite_conn, pg_conn)
