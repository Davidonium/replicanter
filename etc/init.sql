create table people (
	id int unsigned not null primary key,
	first_name varchar(50) not null,
	email varchar(50) not null,
	gender enum ('Male', 'Female') not null,
	birthday DATE not null,
	balance decimal(6,2) not null,
	is_active bit not null default 0,
	latitude decimal(7,4) not null,
	longitude decimal(7,4) not null,
	breakfast_at time not null,
	created_at datetime not null
);


