CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.filmwork (
	id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created timestamp with time zone,
    modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person (
	id uuid PRIMARY KEY,
	full_name TEXT NOT NULL,
	created timestamp with time zone,
	modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre (
	id uuid PRIMARY KEY,
	name TEXT NOT NULL,
	description TEXT,
	created timestamp with time zone,
	modified timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.genre_filmwork (
	id uuid PRIMARY KEY,
	genre_id uuid NOT NULL REFERENCES content.genre,
	filmwork_id uuid NOT NULL REFERENCES content.filmwork,
	created timestamp with time zone
);

CREATE TABLE IF NOT EXISTS content.person_filmwork (
	id uuid PRIMARY KEY,
	filmwork_id uuid NOT NULL REFERENCES content.filmwork,
	person_id uuid NOT NULL REFERENCES content.person,
	role TEXT NOT NULL,
	created timestamp with time zone
);

CREATE INDEX filmwork_creation_date_idx ON content.filmwork(creation_date);
CREATE UNIQUE INDEX filmwork_person_idx ON content.person_filmwork (filmwork_id, person_id);


