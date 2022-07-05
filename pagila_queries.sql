
/* task01 */
select category.name as Категория, count(film_category.film_id) as Количество
from film_category
inner join category on film_category.category_id = category.category_id
group by category.name
order by Количество desc;


/* task02 */
select actor.first_name as Имя, actor.last_name as Фамилия, count(rental_id) as Количество_аренд
from rental
inner join inventory on inventory.inventory_id = rental.inventory_id
inner join film_actor on film_actor.film_id = inventory.film_id
inner join actor on actor.actor_id = film_actor.actor_id
group by actor.actor_id
order by Количество_аренд desc
limit 10;


/* task03 */
select category.name as Категория, sum(payment.amount) as Сумма
from payment
inner join rental on rental.rental_id = payment.rental_id
inner join inventory on inventory.inventory_id = rental.inventory_id
inner join film_category on film_category.film_id = inventory.film_id
inner join category on category.category_id = film_category.category_id
group by category.name
order by Сумма desc
limit 1;


/* task04 */
select film.title as Название
from film
left join inventory on film.film_id = inventory.film_id
where inventory.film_id is null;


/* task05 */
select actor.first_name as Имя, actor.last_name as Фамилия, count(film_actor.film_id) as Количество_фильмов
from actor
inner join film_actor on actor.actor_id = film_actor.actor_id
inner join film_category on film_actor.film_id = film_category.film_id
inner join category on film_category.category_id = category.category_id
where category.name = 'Children'
group by actor.actor_id
having count(film_actor.film_id) in (
    select distinct count(film_actor.film_id) as num_of_films
    from actor
    inner join film_actor on actor.actor_id = film_actor.actor_id
    inner join film_category on film_actor.film_id = film_category.film_id
    inner join category on film_category.category_id = category.category_id
    where category.name = 'Children'
    group by actor.actor_id
    order by num_of_films desc
    limit 3
    )
order by Количество_фильмов desc;


/* task06 */
select city.city as Город, sum(case when customer.active = 1 then 1 else 0 end) as Активные,  sum(case when customer.active = 0 then 1 else 0 end) as Неактивные
from customer
inner join address on customer.address_id = address.address_id
right join city on address.city_id = city.city_id
group by city.city
order by Неактивные desc;


/* task07 */
    (select category.name as Категория, sum(case
        when (city.city like 'A%' or city.city like 'a%') then rental_duration
        end
        ) as Время_в_городах_на_А,
        sum(case
        when (city.city like '%-%') then rental_duration
        end
        ) as Время_в_городах_с_тире
    from rental
    inner join customer on rental.customer_id = customer.customer_id
    inner join address on address.address_id = customer.address_id
    inner join city on city.city_id = address.city_id
    inner join inventory on rental.inventory_id = inventory.inventory_id
    right join film_category on inventory.film_id = film_category.film_id
    inner join category on film_category.category_id = category.category_id
    inner join film on film.film_id = film_category.film_id
    group by category.name
    order by Время_в_городах_на_А desc
    limit 1
        )
union
    (select category.name as Категория, sum(case
        when (city.city like 'A%' or city.city like 'a%') then rental_duration
        end
        ) as Время_в_городах_на_А,
        sum(case
        when (city.city like '%-%') then rental_duration
        end
        ) as Время_в_городах_с_тире
    from rental
    inner join customer on rental.customer_id = customer.customer_id
    inner join address on address.address_id = customer.address_id
    inner join city on city.city_id = address.city_id
    inner join inventory on rental.inventory_id = inventory.inventory_id
    right join film_category on inventory.film_id = film_category.film_id
    inner join category on film_category.category_id = category.category_id
    inner join film on film.film_id = film_category.film_id
    group by category.name
    order by Время_в_городах_с_тире desc
    limit 1
        )
