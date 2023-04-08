-- Notifications
create table if not exists notifications (
	id serial primary key,
	uuid varchar(60),
	notification text, 
	read boolean default false,
	target text
);
