create table people (
	id int unsigned primary key not null,
	first_name varchar(50) not null,
	last_name varchar(50) not null,
	email varchar(50) not null,
	gender varchar(50) not null,
	ip_address varchar(20) not null,
	created_at datetime not null
);