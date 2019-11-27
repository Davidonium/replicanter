create table doctors
(
	id int unsigned not null auto_increment primary key,
	name varchar(100) not null,
	surname varchar(100) not null
);

insert into doctors (name, surname) values ('David', 'Hernando');
insert into doctors (name, surname) values ('Antoine', 'Meeus');
insert into doctors (name, surname) values ('Juan Francisco', 'Sanchez');
insert into doctors (name, surname) values ('David', 'Hernando');