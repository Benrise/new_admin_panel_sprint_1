from enum import Enum
import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from utils.movie_dataclasses import Genre, Filmwork, GenreFilmwork, PersonFilmwork, Person


class TablesNamesEnum(Enum):
    FILM_WORK = 'film_work'
    GENRE = 'genre'
    GENRE_FILM_WORK = 'genre_film_work'
    PERSON = 'person'
    PERSON_FILM_WORK = 'person_film_work'


class DataTable:
    name: str
    data: list

    def __init__(self, name, data):
        self.name = name
        self.data = data


class DataTables:
    film_work: DataTable
    genre: DataTable
    genre_film_work: DataTable
    person: DataTable
    person_film_work: DataTable

    def __init__(self,
                 film_work,
                 genre,
                 genre_film_work,
                 person,
                 person_film_work):
        self.film_work = film_work
        self.genre = genre
        self.genre_film_work = genre_film_work
        self.person = person
        self.person_film_work = person_film_work


class SQLiteExtractor:
    connection: sqlite3.Connection

    def __init__(self, connection):
        self.connection = connection

    def extract_movies(self):
        db_path = 'db.sqlite'
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        curs = conn.cursor()
        curs.execute("SELECT * FROM sqlite_master where type='table';")
        tables = curs.fetchall()
        for table in tables:
            if (table[1] == 'film_work'):
                film_work_data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = Filmwork(row['id'],
                                      row['title'],
                                      row['description'],
                                      row['creation_date'],
                                      row['file_path'],
                                      row['rating'],
                                      row['type'],
                                      row['created_at'],
                                      row['updated_at'])
                    film_work_data.append(record)

            if (table[1] == 'genre'):
                genre_data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = Genre(row['id'],
                                   row['name'],
                                   row['description'],
                                   row['created_at'],
                                   row['updated_at'])
                    genre_data.append(record)

            if (table[1] == 'genre_film_work'):
                genre_film_work_data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = GenreFilmwork(row['id'],
                                           row['film_work_id'],
                                           row['genre_id'],
                                           row['created_at'])
                    genre_film_work_data.append(record)

            if (table[1] == 'person'):
                person_data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = Person(row['id'],
                                    row['full_name'],
                                    row['created_at'],
                                    row['updated_at'])
                    person_data.append(record)

            if (table[1] == 'person_film_work'):
                person_film_work_data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = PersonFilmwork(row['id'],
                                            row['film_work_id'],
                                            row['person_id'],
                                            row['role'],
                                            row['created_at'])
                    person_film_work_data.append(record)
        conn.close()
        data_tables = DataTables(film_work=DataTable(TablesNamesEnum.FILM_WORK.name, film_work_data),
                                 genre=DataTable(TablesNamesEnum.GENRE.name, genre_data),
                                 genre_film_work=DataTable(TablesNamesEnum.GENRE_FILM_WORK.name, genre_film_work_data),
                                 person=DataTable(TablesNamesEnum.PERSON.name, person_data),
                                 person_film_work=DataTable(TablesNamesEnum.PERSON_FILM_WORK.name, person_film_work_data)
                                 )
        return data_tables


class PostgresSaver:
    connection: _connection

    def __init__(self, connection):
        self.connection = connection

    def save_all_data(self, data_tables: DataTables):
        curs = self.connection.cursor()
        args = []
        for item in data_tables.genre.data:
            args.append(curs.mogrify("(%s, %s, %s, %s, %s)",
                                     (item.id,
                                      item.name,
                                      item.description,
                                      item.created_at,
                                      item.updated_at)
                                     ).decode())

        args_str = ','.join(args)
        curs.execute(f"""
            INSERT INTO content.{data_tables.genre.name} (id,
            name,
            description,
            created_at,
            updated_at)
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
            """)

        args = []
        for item in data_tables.film_work.data:
            args.append(curs.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                     (item.id,
                                      item.title,
                                      item.description,
                                      item.creation_date,
                                      item.file_path,
                                      item.type,
                                      item.rating,
                                      item.created_at,
                                      item.updated_at)).decode())

        args_str = ','.join(args)
        curs.execute(f"""
            INSERT INTO content.{data_tables.film_work.name} (id,
            title,
            description,
            creation_date,
            file_path,
            type,
            rating,
            created_at,
            updated_at)
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
            """)

        args = []
        for item in data_tables.genre_film_work.data:
            args.append(curs.mogrify("(%s, %s, %s, %s)",
                                     (item.id,
                                      item.film_work_id,
                                      item.genre_id,
                                      item.created_at)).decode())

        args_str = ','.join(args)
        curs.execute(f"""
            INSERT INTO content.{data_tables.genre_film_work.name} (id,
            film_work_id,
            genre_id,
            created_at)
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
            """)

        args = []
        for item in data_tables.person.data:
            args.append(curs.mogrify("(%s, %s, %s, %s)",
                                     (item.id, 
                                      item.full_name,
                                      item.created_at,
                                      item.updated_at)).decode())
        args_str = ','.join(args)
        curs.execute(f"""
            INSERT INTO content.{data_tables.person.name} (id,
            full_name,
            created_at,
            updated_at)
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
            """)

        args = []
        for item in data_tables.person_film_work.data:
            args.append(curs.mogrify("(%s, %s, %s, %s, %s)",
                                     (item.id,
                                      item.film_work_id,
                                      item.person_id,
                                      item.role,
                                      item.created_at)).decode())
        args_str = ','.join(args)
        curs.execute(f"""
            INSERT INTO content.{data_tables.person_film_work.name} (id,
            film_work_id,
            person_id,
            role,
            created_at)
            VALUES {args_str}
            ON CONFLICT (id) DO NOTHING
            """)

        curs.close()


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == '__main__':
    dsl = {'dbname': 'movies_database', 'user': 'app', 'password': '123qwe', 'host': '127.0.0.1', 'port': 5433}
    with sqlite3.connect('db.sqlite') as sqlite_conn, psycopg2.connect(**dsl, cursor_factory=DictCursor) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
