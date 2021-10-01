--Используя демо базу данных, напишите запросы для того, чтобы:

-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.category_id, c."name", count(distinct fc.film_id)
from film_category fc
join category c on c.category_id  = fc.category_id 
group by c.category_id, c."name"
order by 3 desc;

-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select a.actor_id, a.first_name || ' ' || a.last_name as fullname, sum(f.cnt_rent) sum_rent
from (
	select i.film_id, count(r.rental_id) cnt_rent 
	from rental r
	join inventory i on i.inventory_id = r.inventory_id 
	group by i.film_id) f
join film_actor fa on fa.film_id = f.film_id
join actor a on a.actor_id = fa.actor_id 
group by a.actor_id, a.first_name, a.last_name
order by sum_rent desc;


-- 3. вывести категорию фильмов, на которую потратили больше всего денег.
select cat.category_id , cat."name", cat.sum_amount
from (select c.category_id , c."name", sum(p.amount) sum_amount
		from film_category fc
		join category c ON c.category_id = fc.category_id 
		join inventory i on fc.film_id  = i.film_id 
		join rental r on r.inventory_id = i.inventory_id 
		join payment p on p.rental_id = r.rental_id 
		group by c.category_id , c."name" order by sum_amount desc) cat
limit 1;


--4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.film_id , f.title 
from film_category fc 
join film f on f.film_id =fc.film_id 
left join inventory i on i.film_id = fc.film_id 
where i.film_id  is null;


--5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
with cte as (
	select a.actor_id , a.first_name || ' ' || a.last_name as full_name, count(distinct fc.film_id) cnt_films
	from film_category fc
	join category c on c.category_id = fc.category_id 
	join film_actor fa on fa.film_id  = fc.film_id 
	join actor a on a.actor_id  = fa.actor_id 
	where c."name" = 'Children'
	group by a.actor_id , a.first_name, a.last_name
)

select distinct c1.actor_id, c1.full_name, c1.cnt_films
from cte c1
join (select c2.actor_id, c2.cnt_films
		from cte c2
		order by 2 desc
		limit 3) c2 on c1.cnt_films = c2.cnt_films;


-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.
select ci.city_id, ci.city, case when c.active =1 then 'активный' else 'не активный' end active, count(distinct c.customer_id) cnt_cust
from customer c
join address a on a.address_id  =c.address_id 
join city ci on ci.city_id  = a.city_id 
group by ci.city_id, ci.city, c.active
order by c.active, cnt_cust desc;

-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. 
--    То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

with cte as 
(
	select  c.category_id, c."name", 
			sum(case when f.title ilike 'a%' then floor(EXTRACT(EPOCH FROM (return_date - rental_date))/3600) else 0 end) sum_time_1,
			sum(case when ci.city like '%-%' then floor(EXTRACT(EPOCH FROM (return_date - rental_date))/3600) else 0 end) sum_time_2,
			1 as type
	from rental r
	join customer cust on cust.customer_id  = r.customer_id 
	join address a on a.address_id  = cust.address_id 
	join city ci on ci.city_id  = a.city_id 
	join inventory i on i.inventory_id = r.rental_id 
	join film_category fc on fc.film_id  = i.film_id 
	join film f on f.film_id  = fc.film_id 
	join category c on c.category_id  = fc.category_id 
	group by c.category_id, c."name"
)

select c.category_id, c."name", c.sum_time_1, 'где фильны начинаются на букву “a”' as type
from cte c
where exists (select max(c1.sum_time_1) m1 from cte c1 having max(c1.sum_time_1) = c.sum_time_1 )

union all

select c.category_id, c."name", c.sum_time_2, 'для городов в которых есть символ “-”'as type
from cte c
where exists (select max(c1.sum_time_2) m1 from cte c1 having max(c1.sum_time_2) = c.sum_time_2 );

