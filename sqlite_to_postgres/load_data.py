from enum import Enum
import sqlite3
import subprocess

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2 import DatabaseError
from psycopg2.extras import DictCursor
from utils.movie_dataclasses import (
    Genre,
    Filmwork,
    GenreFilmwork,
    PersonFilmwork,
    Person
)
from dataclasses import fields, dataclass


class TablesNamesEnum(Enum):
    FILM_WORK = 'film_work'
    GENRE = 'genre'
    GENRE_FILM_WORK = 'genre_film_work'
    PERSON = 'person'
    PERSON_FILM_WORK = 'person_film_work'


@dataclass
class DataTable:
    name: str
    data: list


@dataclass
class DataTables:
    film_work: DataTable
    genre: DataTable
    genre_film_work: DataTable
    person: DataTable
    person_film_work: DataTable


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def _execute_query(self, query):
        try:
            curs = self.connection.cursor()
            curs.execute(query)
            return curs.fetchall()
        except sqlite3.Error as e:
            print(f"SQLite error while executing the query: {e}")
            raise

    def _get_tables(self):
        query = "SELECT * FROM sqlite_master WHERE type='table';"
        return self._execute_query(query)

    def _create_records(self, rows, record_class):
        records = []
        for row in rows:
            try:
                record = record_class(*row)
                records.append(record)
            except Exception as e:
                print(f"Error on creating a record: {e}")
                raise
        return records

    def extract_movies(self):
        try:
            film_work_data = []
            genre_data = []
            genre_film_work_data = []
            person_data = []
            person_film_work_data = []

            tables = self._get_tables()
            for table in tables:
                table_name = table[1]
                query = f"SELECT * FROM {table_name}"
                rows = self._execute_query(query)

                if table_name == TablesNamesEnum.FILM_WORK.value:
                    film_work_data = self._create_records(rows, Filmwork)

                elif table_name == TablesNamesEnum.GENRE.value:
                    genre_data = self._create_records(rows, Genre)

                elif table_name == TablesNamesEnum.GENRE_FILM_WORK.value:
                    genre_film_work_data = self._create_records(rows, GenreFilmwork)

                elif table_name == TablesNamesEnum.PERSON.value:
                    person_data = self._create_records(rows, Person)

                elif table_name == TablesNamesEnum.PERSON_FILM_WORK.value:
                    person_film_work_data = self._create_records(rows, PersonFilmwork)

            data_tables = DataTables(
                DataTable(TablesNamesEnum.FILM_WORK.name, film_work_data),
                DataTable(TablesNamesEnum.GENRE.name, genre_data),
                DataTable(TablesNamesEnum.GENRE_FILM_WORK.name, genre_film_work_data),
                DataTable(TablesNamesEnum.PERSON.name, person_data),
                DataTable(TablesNamesEnum.PERSON_FILM_WORK.name, person_film_work_data))

            return data_tables

        except Exception as e:
            print(f"Unexpected error: {e}")
            raise



class PostgresSaver:
    def __init__(self, connection: _connection):
        self.connection = connection

    def save_all_data(self, data_tables: DataTables):
        curs = self.connection.cursor()
        insert_table_records(data_tables.genre, curs)
        insert_table_records(data_tables.film_work, curs)
        insert_table_records(data_tables.genre_film_work, curs)
        insert_table_records(data_tables.person, curs)
        insert_table_records(data_tables.person_film_work, curs)
        curs.close()


def insert_table_records(data_table: DataTable, curs: DictCursor):
    try:
        args = []
        attribute_names = []
        for item in data_table.data:
            item_fields = fields(item)
            attribute_names = [field.name for field in item_fields]
            values = [getattr(item, field) for field in attribute_names]
            args.append(curs.mogrify(f"({', '.join(['%s']*len(attribute_names))})", tuple(values)).decode())
        args_str = ','.join(args)
        attribute_names_str = ', '.join(attribute_names)

        curs.execute(f"""
            INSERT INTO content.{data_table.name} ({attribute_names_str})
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
        """)

        curs.connection.commit()

    except DatabaseError as e:
        print(f"Database error while insert data: {e}")
        curs.connection.rollback()

    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5433}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        print('Data transfer has started...')
        load_from_sqlite(sqlite_conn, pg_conn)
        print('Data transfer is completed!')
    run_tests = input("Start tests? (y/n): ").lower()
    if run_tests == 'y':
        try:
            print("Running tests...")
            subprocess.run(['python', 'tests/check_consistency/main.py'])
        except Exception as e:
            print(f"Error on running tests: {e}")
