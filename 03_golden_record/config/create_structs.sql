create schema v_dvd_rental;

create materialized view v_dvd_rental.customers_mv as
select c.customer_id, c.first_name, c.last_name, case when c.active = 1 then true else false end is_active, 
a.address, a.district,ct.city, ctr.country, trim(a.address)||', '||trim(ct.city)||', '||trim(ctr.country)||' ('||trim(a.district)||')' full_address,
a.phone, c.email, a.postal_code, 
sf.first_name as assistant_name, sf.last_name as assistant_last_name, sf.email as assistant_email,
now() last_refresh_date
from dvd_rental.customer c 
left join dvd_rental.address a on c.address_id = a.address_id
left join dvd_rental.city ct on a.city_id = ct.city_id 
left join dvd_rental.country ctr on ct.country_id = ctr.country_id 
left join dvd_rental.store s on c.store_id = s.store_id 
left join dvd_rental.staff sf on s.manager_staff_id = sf.staff_id 
where c.active = 1;

create unique index idx_mv_customers_customer on v_dvd_rental.customers_mv(customer_id);

create materialized view v_dvd_rental.last_rentals_mv as 
with rentals as (
	select r.rental_id, r.customer_id, f.title, ct.name category, coalesce(p.amount, f.rental_rate) as amount, r.rental_date, r.return_date, 
	extract(day from age(coalesce(r.return_date, now()), r.rental_date)) rental_duration, case when r.return_date is not null then true else false end is_completed,
	case when extract(day from age(coalesce(r.return_date, now()), r.rental_date)) > f.rental_duration then true else false end is_overdue,
	trim(a.address)||', '||trim(ca.city)||', '||trim(ctr.country)||' ('||trim(a.district)||')' as store, row_number() over(partition by r.customer_id order by rental_date desc) rental_cnt
	from dvd_rental.rental r 
	left join dvd_rental.customer c on r.customer_id = c.customer_id
	left join dvd_rental.payment p on r.rental_id = p.rental_id 
	left join dvd_rental.inventory i on r.inventory_id = i.inventory_id
	left join dvd_rental.film f on i.film_id = f.film_id
	left join dvd_rental.film_category fc on f.film_id = fc.film_id
	left join dvd_rental.category ct on fc.category_id = ct.category_id
	left join dvd_rental.store s on i.store_id = s.store_id 
	left join dvd_rental.address a on s.address_id = a.address_id 
	left join dvd_rental.city ca on a.city_id = ca.city_id 
	left join dvd_rental.country ctr on ca.country_id = ctr.country_id 
	where c.active = 1
)
select rental_id, customer_id, title, category, amount, rental_date, return_date, rental_duration, is_completed, is_overdue, store, now() last_refresh_date 
from rentals
where rental_cnt <= 10;

create unique index idx_mv_last_rentals_id on v_dvd_rental.last_rentals_mv(rental_id);
create index idx_mv_last_rentals_customer on v_dvd_rental.last_rentals_mv(customer_id);

create materialized view v_dvd_rental.customer_aggr_mv as 
with rentals as (
	select r.rental_id, r.customer_id, f.film_id, f.title, f.release_year, f.length, ct.name category, coalesce(p.amount, f.rental_rate) as amount, r.rental_date, r.return_date, 
	extract(day from age(coalesce(r.return_date, now()), r.rental_date)) rental_duration, 
	case when extract(day from age(coalesce(r.return_date, now()), r.rental_date)) > f.rental_duration then 1 else 0 end is_overdue,
	trim(a.address)||' '||trim(ca.city)||', '||trim(ctr.country)||' ('||trim(a.district)||')' as store, row_number() over(partition by r.customer_id order by rental_date desc) rental_cnt
	from dvd_rental.rental r 
	left join dvd_rental.customer c on r.customer_id = c.customer_id
	left join dvd_rental.payment p on r.rental_id = p.rental_id 
	left join dvd_rental.inventory i on r.inventory_id = i.inventory_id
	left join dvd_rental.film f on i.film_id = f.film_id
	left join dvd_rental.film_category fc on f.film_id = fc.film_id
	left join dvd_rental.category ct on fc.category_id = ct.category_id
	left join dvd_rental.store s on i.store_id = s.store_id 
	left join dvd_rental.address a on s.address_id = a.address_id 
	left join dvd_rental.city ca on a.city_id = ca.city_id 
	left join dvd_rental.country ctr on ca.country_id = ctr.country_id 
	where c.active = 1
), store as (
	select r.customer_id, r.store, count(1) as rental_cnt, row_number() over(partition by r.customer_id order by count(1) desc, max(r.rental_date) desc) rn 
	from rentals r
	group by r.customer_id, r.store
), categories as (
	select r.customer_id, r.category, count(1) as rental_cnt, row_number() over(partition by r.customer_id order by count(1) desc, max(r.rental_date) desc) rn 
	from rentals r
	group by r.customer_id, r.category
), film as (
	select r.customer_id, r.title, count(1) as rental_cnt, row_number() over(partition by r.customer_id order by count(1) desc, max(r.rental_date) desc) rn 
	from rentals r
	group by r.customer_id, r.title
), year_film as (
	select r.customer_id, r.release_year, count(1) as rental_cnt, row_number() over(partition by r.customer_id order by count(1) desc, max(r.rental_date) desc) rn 
	from rentals r
	group by r.customer_id, r.release_year
), actor as (
	select r.customer_id, trim(a.first_name)||' '||trim(a.last_name) actor_name, count(1) as rental_cnt, row_number() over(partition by r.customer_id order by count(1) desc, max(r.rental_date) desc) rn 
	from rentals r
	left join dvd_rental.film_actor f on r.film_id = f.film_id
	left join dvd_rental.actor a on f.actor_id = a.actor_id
	group by r.customer_id, actor_name
)
select r.customer_id, sum(r.is_overdue)*100/count(1) overdue_score, 
	max(case when s.rn = 1 then s.store||' ('||s.rental_cnt||')' else null end) most_recent_store,
	max(case when r.rental_cnt = 1 then r.title||' ('||r.category||')' else null end) last_rental_film,
	max(case when r.rental_cnt = 1 then r.rental_date else null end) last_rental_date,
	sum(r.amount) lifetime_value,
	count(1) total_rental_count,
	cast(avg(r.rental_duration) as numeric(10,2)) average_rental_duration,
	cast(avg(r.amount) as numeric(10,2)) average_rental_payment,
	cast(avg(r.length) as numeric(10,2)) average_film_duration,
	sum(case when r.rental_date >= now() - interval '1 year' then 1 else 0 end) last_year_rental_count,
	sum(case when r.rental_date >= now() - interval '1 year' then r.amount else 0 end) last_year_payments_sum,
	max(case when r.rental_cnt = 1 then r.amount else null end) last_payment,
	max(case when c.rn = 1 then c.category||' ('||c.rental_cnt||')' else null end) most_recent_category,
	max(case when c.rn = 2 then c.category||' ('||c.rental_cnt||')' else null end) second_most_recent_category,
	max(case when c.rn = 3 then c.category||' ('||c.rental_cnt||')' else null end) third_most_recent_category,
	max(case when f.rn = 1 then f.title||' ('||f.rental_cnt||')' else null end) most_recent_film_title,
	max(case when a.rn = 1 then a.actor_name||' ('||f.rental_cnt||')' else null end) most_recent_film_actor,
	max(case when y.rn = 1 then y.release_year||' ('||y.rental_cnt||')' else null end) most_recent_film_year,
	now() last_refresh_date
from rentals r
left join store s on r.customer_id = s.customer_id and s.rn = 1
left join categories c on r.customer_id = c.customer_id and c.rn <= 3
left join film f on r.customer_id = f.customer_id and f.rn = 1
left join year_film y on r.customer_id = y.customer_id and y.rn = 1
left join actor a on r.customer_id = a.customer_id and a.rn = 1
group by r.customer_id;

create unique index idx_mv_customer_aggr_customer on v_dvd_rental.customer_aggr_mv(customer_id);

	