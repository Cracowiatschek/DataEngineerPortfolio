alter table dvd_rental.city
add column latitude float not null default(0.0) CHECK (
    latitude BETWEEN -90 AND 90
);

alter table dvd_rental.city
add column longitude float not null default(0.0) CHECK (
    longitude BETWEEN -180 AND 180
);

alter table dvd_rental.city
add column is_set bool not null default(false);
