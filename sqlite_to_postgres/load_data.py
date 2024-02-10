import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor
from utils.movie_dataclasses import Genre, Filmwork, GenreFilmwork, PersonFilmwork, Person


class DataTable:
    name: str
    data: list

    def __init__(self, name, data):
        self.name = name
        self.data = data


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
        data_tables = []
        for table in tables:
            if (table[1] == 'genre'):
                data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = Genre(row['id'],
                                   row['name'],
                                   row['description'],
                                   row['created_at'],
                                   row['updated_at'])
                    data.append(record)
                genre_table = DataTable(table[1], data)
                data_tables.append(genre_table)

            if (table[1] == 'film_work'):
                data = []
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
                    data.append(record)
                film_work_table = DataTable(table[1], data)
                data_tables.append(film_work_table)

            if (table[1] == 'genre_film_work'):
                data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = GenreFilmwork(row['id'],
                                           row['film_work_id'],
                                           row['genre_id'],
                                           row['created_at'])
                    data.append(record)
                genre_film_work_table = DataTable(table[1], data)
                data_tables.append(genre_film_work_table)

            if (table[1] == 'person'):
                data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = Person(row['id'],
                                    row['full_name'],
                                    row['created_at'],
                                    row['updated_at'])
                    data.append(record)
                person = DataTable(table[1], data)
                data_tables.append(person)

            if (table[1] == 'person_film_work'):
                data = []
                for row in curs.execute(f"SELECT * FROM {table[1]}"):
                    record = PersonFilmwork(row['id'],
                                            row['film_work_id'],
                                            row['person_id'],
                                            row['role'],
                                            row['created_at'])
                    data.append(record)
                person_film_work_table = DataTable(table[1], data)
                data_tables.append(person_film_work_table)
        conn.close()
        return data_tables


class PostgresSaver:
    connection: _connection

    def __init__(self, connection):
        self.connection = connection

    def save_all_data(self, data_tables: list[DataTable]):
        curs = self.connection.cursor()
        for table in data_tables:
            if (table.name == 'genre'):
                args = []
                for item in table.data:
                    args.append(curs.mogrify("(%s, %s, %s, %s, %s)",
                                             (item.id,
                                              item.name,
                                              item.description,
                                              item.created_at,
                                              item.updated_at)
                                             ).decode())

                args_str = ','.join(args)
                curs.execute(f"""
                    INSERT INTO content.{table.name} (id,
                    name,
                    description,
                    created_at,
                    updated_at)
                    VALUES {args_str}
                    ON CONFLICT (id) DO NOTHING
                    """)

            if (table.name == 'film_work'):
                args = []
                for item in table.data:
                    args.append(curs.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                             (item.id,
                                              item.title,
                                              item.description,
                                              item.creation_date,
                                              item.file_path,
                                              item.type,
                                              item.rating,
                                              item.created_at,
                                              item.updated_at)
                                             ).decode())

                args_str = ','.join(args)
                curs.execute(f"""
                    INSERT INTO content.{table.name} (id,
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

            if (table.name == 'genre_film_work'):
                args = []
                for item in table.data:
                    args.append(curs.mogrify("(%s, %s, %s, %s)",
                                             (item.id,
                                              item.film_work_id,
                                              item.genre_id,
                                              item.created_at)
                                             ).decode())

                args_str = ','.join(args)
                curs.execute(f"""
                    INSERT INTO content.{table.name} (id,
                    film_work_id,
                    genre_id,
                    created_at)
                    VALUES {args_str}
                    ON CONFLICT (id) DO NOTHING
                    """)

            if (table.name == 'person'):
                args = []
                for item in table.data:
                    args.append(curs.mogrify("(%s, %s, %s, %s)",
                                             (item.id,
                                              item.full_name,
                                              item.created_at,
                                              item.updated_at)
                                             ).decode())
                args_str = ','.join(args)
                curs.execute(f"""
                    INSERT INTO content.{table.name} (id,
                    full_name,
                    created_at,
                    updated_at)
                    VALUES {args_str}
                    ON CONFLICT (id) DO NOTHING
                    """)

            if (table.name == 'person_film_work'):
                args = []
                for item in table.data:
                    args.append(curs.mogrify("(%s, %s, %s, %s, %s)",
                                             (item.id,
                                              item.film_work_id,
                                              item.person_id,
                                              item.role,
                                              item.created_at)
                                             ).decode())
                args_str = ','.join(args)
                curs.execute(f"""
                    INSERT INTO content.{table.name} (id,
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
