create schema dvd_rental;

create or replace function update_last_update_column()
returns trigger as $$
begin
    new.last_update := current_timestamp;
    return new;
end;
$$ language plpgsql;

create table dvd_rental.country (
	country_id int generated always as identity primary key,
	country varchar(50) not null, 
	last_update timestamp not null default(now())
);

create trigger set_last_update
before update on dvd_rental.country
for each row
execute function update_last_update_column();

create table dvd_rental.city (
	city_id int generated always as identity primary key,
	city varchar(50) not null,
	country_id smallint not null,
	last_update timestamp not null default(now()),
	constraint fk_country
		foreign key (country_id)
		references dvd_rental.country(country_id)
		on delete cascade
);

create index idx_city_fk_country_id on dvd_rental.city(country_id);

create trigger set_last_update
before update on dvd_rental.city
for each row
execute function update_last_update_column();

create table dvd_rental.address (
	address_id int generated always as identity primary key,
	address varchar(50) not null,
	address2 varchar(50),
	district varchar(20) not null,
	city_id smallint not null,
	postal_code varchar(10),
	phone varchar(20) not null,
	last_update timestamp not null default(now()),
	constraint fk_city
		foreign key (city_id)
		references dvd_rental.city(city_id)
		on delete cascade
);

create index idx_address_fk_city_id on dvd_rental.address(city_id);

create trigger set_last_update
before update on dvd_rental.address
for each row
execute function update_last_update_column();

create table dvd_rental.store (
	store_id int generated always as identity primary key,
	manager_staff_id smallint not null,
	address_id smallint not null,
	last_update timestamp not null default(now()),
	constraint fk_address
		foreign key (address_id)
		references dvd_rental.address(address_id)
		on delete cascade
);

create index idx_store_fk_address_id on dvd_rental.store(address_id);
create index idx_store_fk_manager_staff_id on dvd_rental.store(manager_staff_id);

create trigger set_last_update
before update on dvd_rental.store
for each row
execute function update_last_update_column();

create table dvd_rental.staff (
	staff_id int generated always as identity primary key,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address_id smallint not null,
	email varchar(50),
	store_id smallint not null,
	active boolean not null default(true),
	username varchar(16) not null,
	password varchar(40),
	last_update timestamp not null default(now()),
	picture bytea,
	constraint fk_address
		foreign key (address_id)
		references dvd_rental.address(address_id)
		on delete cascade
);

alter table dvd_rental.store
add constraint fk_manager_staff_id
	foreign key (manager_staff_id)
	references dvd_rental.staff(staff_id)
	on delete cascade;

create trigger set_last_update
before update on dvd_rental.staff
for each row
execute function update_last_update_column();

create table dvd_rental.customer (
	customer_id int generated always as identity primary key,
	store_id smallint not null,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	address_id smallint not null,
	email varchar(50),
	activebool boolean not null default(true),
	create_date date not null default(now()),
	last_update timestamp default(now()),
	active int,
	constraint fk_address
		foreign key (address_id)
		references dvd_rental.address(address_id)
		on delete cascade
);

create trigger set_last_update
before update on dvd_rental.customer
for each row
execute function update_last_update_column();

create index idx_customer_last_name on dvd_rental.customer(last_name);
create index idx_customer_store_id on dvd_rental.customer(store_id);

create table dvd_rental.category (
	category_id int generated always as identity primary key,
	name varchar(25) not null,
	last_update timestamp not null default(now())
);

create trigger set_last_update
before update on dvd_rental.category
for each row
execute function update_last_update_column();

create table dvd_rental.language (
	language_id int generated always as identity primary key,
	name varchar(20) not null,
	last_update timestamp not null default(now())
);

create trigger set_last_update
before update on dvd_rental.language
for each row
execute function update_last_update_column();

create table dvd_rental.film (
	film_id int generated always as identity primary key,
	title varchar(255) not null,
	description text,
	release_year int,
	language_id smallint not null,
	rental_duration smallint not null default(3),
	rental_rate numeric(4,2) not null default(4.99),
	length smallint,
	replacement_cost numeric(5,2) not null default(19.99),
	rating text check(rating in ('G', 'PG', 'PG-13', 'R', 'NC-17')),
	last_update timestamp not null default(now()),
	special_features text,
	fulltext tsvector not null,
	constraint fk_language
		foreign key (language_id)
		references dvd_rental.language(language_id)
		on delete cascade
);

create index idx_film_fulltext on dvd_rental.film(fulltext);
create index idx_film_language_id on dvd_rental.film(language_id);
create index idx_film_title on dvd_rental.film(title);

create trigger set_last_update
before update on dvd_rental.film
for each row
execute function update_last_update_column();

create table dvd_rental.film_category (
	film_id smallint not null primary key,
	category_id smallint not null,
	last_update timestamp not null default(now()),
	constraint fk_film
		foreign key (film_id)
		references dvd_rental.film(film_id)
		on delete cascade,
	constraint fk_category
		foreign key (category_id)
		references dvd_rental.category(category_id)
		on delete cascade
);

create trigger set_last_update
before update on dvd_rental.film_category
for each row
execute function update_last_update_column();

create table dvd_rental.actor (
	actor_id int generated always as identity primary key,
	first_name varchar(45) not null,
	last_name varchar(45) not null,
	last_update timestamp not null default(now())
);

create index idx_actor on dvd_rental.actor(last_name);

create trigger set_last_update
before update on dvd_rental.actor
for each row
execute function update_last_update_column();

create table dvd_rental.film_actor (
	film_id smallint not null,
	actor_id smallint not null,
	last_update timestamp not null default(now()),
	primary key(film_id, actor_id),
	constraint fk_film
		foreign key (film_id)
		references dvd_rental.film(film_id)
		on delete cascade,
	constraint fk_actor
		foreign key (actor_id)
		references dvd_rental.actor(actor_id)
		on delete cascade
);

create trigger set_last_update
before update on dvd_rental.film_actor
for each row
execute function update_last_update_column();

create table dvd_rental.inventory (
	inventory_id int generated always as identity primary key,
	film_id smallint not null,
	store_id smallint not null,
	last_update timestamp not null default(now()),
	constraint fk_film
		foreign key (film_id)
		references dvd_rental.film(film_id)
		on delete cascade,
	constraint fk_store
		foreign key (store_id)
		references dvd_rental.store(store_id)
		on delete cascade
);

create trigger set_last_update
before update on dvd_rental.inventory
for each row
execute function update_last_update_column();

create index idx_store_film on dvd_rental.inventory(store_id, film_id);

create table dvd_rental.rental (
	rental_id int generated always as identity primary key,
	rental_date timestamp not null,
	inventory_id int not null,
	customer_id smallint not null,
	return_date timestamp,
	staff_id smallint not null,
	last_update timestamp not null default(now()),
	constraint fk_inventory
		foreign key (inventory_id)
		references dvd_rental.inventory(inventory_id)
		on delete cascade,
	constraint fk_customer
		foreign key (customer_id)
		references dvd_rental.customer(customer_id)
		on delete cascade,
	constraint fk_staff
		foreign key (staff_id)
		references dvd_rental.staff(staff_id)
		on delete cascade
);

create trigger set_last_update
before update on dvd_rental.rental
for each row
execute function update_last_update_column();

create index idx_rental_inventory on dvd_rental.rental(inventory_id);
create unique index uidx_rental_rental_date_inventory_customer on dvd_rental.rental(rental_date, inventory_id, customer_id);

create table dvd_rental.payment (
	payment_id int generated always as identity primary key,
	customer_id smallint not null,
	staff_id smallint not null,
	rental_id int not null,
	amount numeric(5,2) not null,
	payment_date timestamp not null,
	constraint fk_rental
		foreign key (rental_id)
		references dvd_rental.rental(rental_id)
		on delete cascade,
	constraint fk_customer
		foreign key (customer_id)
		references dvd_rental.customer(customer_id)
		on delete cascade,
	constraint fk_staff
		foreign key (staff_id)
		references dvd_rental.staff(staff_id)
		on delete cascade
);

create index idx_payment_customer_id on dvd_rental.payment(customer_id);
create index idx_payment_rental_id on dvd_rental.payment(rental_id);
create index idx_payment_staff_id on dvd_rental.payment(staff_id);
