create table urns (
	id varchar not null primary key,
	urn varchar not null,
	created date not null,
	url varchar,
	db_created timestamp not null default current_timestamp
);