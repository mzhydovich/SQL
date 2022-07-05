import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, when

# start spark session
spark = SparkSession.builder.appName("PySpark_tasks").config("spark.jars", "/home/magzim/innowise/py/spark/postgresql-42.2.6.jar").master("local").getOrCreate()

# create database table loader 
db_loader = spark.read.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver") \
    .option("user", "postgres").option("password", "password") # put your username and password

# load tables
actor = db_loader.option('dbtable', 'actor').load()
address = db_loader.option('dbtable', 'address').load()
category = db_loader.option('dbtable', 'category').load()
city = db_loader.option('dbtable', 'city').load()
country = db_loader.option('dbtable', 'country').load()
customer = db_loader.option('dbtable', 'customer').load()
film = db_loader.option('dbtable', 'film').load()
film_actor = db_loader.option('dbtable', 'film_actor').load()
film_category = db_loader.option('dbtable', 'film_category').load()
inventory = db_loader.option('dbtable', 'inventory').load()
language = db_loader.option('dbtable', 'language').load()
payment = db_loader.option('dbtable', 'payment').load()
rental = db_loader.option('dbtable', 'rental').load()
staff = db_loader.option('dbtable', 'staff').load()
store = db_loader.option('dbtable', 'store').load()


# task1

print("Output for 1 task")

film_category.join(category, film_category.category_id == category.category_id, 'left').groupby('name').count().sort(col('count').desc()).show()
	
# task2

print("Output for 2 task")

rental.join(inventory, inventory.inventory_id == rental.inventory_id, 'inner') \
	  .join(film_actor, film_actor.film_id == inventory.film_id, 'inner') \
	  .join(actor, actor.actor_id == film_actor.actor_id, 'inner') \
	  .groupby(['first_name', 'last_name']) \
	  .count() \
	  .orderBy(col('count').desc()) \
	  .show(10)	

# task3

print("Output for 3 task")

payment.join(rental, rental.rental_id == payment.rental_id) \
	   .join(inventory, inventory.inventory_id == rental.inventory_id) \
	   .join(film_category, film_category.film_id == inventory.film_id) \
	   .join(category, category.category_id == film_category.category_id) \
	   .groupby('name') \
	   .sum('amount') \
	   .sort(col('sum(amount)').desc()) \
	   .show(1)

# task4

print("Output for 4 task")

film.join(inventory, inventory.film_id == film.film_id, 'left').where(col('inventory_id').isNull()).select('title').show()

# task5

print("Output for 5 task")

children_actors = film_category.join(film_actor, film_actor.film_id == film_category.film_id, 'inner') \
    .join(actor, actor.actor_id == film_actor.actor_id, 'inner') \
    .join(category, category.category_id == film_category.category_id, 'inner') \
    .where(col('name') == 'Children') \
    .groupby('first_name', 'last_name').count() \
    .sort(col('count').desc())

counts = children_actors.select('count').distinct().rdd.map(lambda c: c[0]).collect()

children_actors.filter(col('count') >= counts[2]).show()

# task6

print("Output for 6 task")

customer.join(address, address.address_id == customer.address_id, 'inner') \
    .join(city, city.city_id == address.city_id, 'inner') \
    .withColumn('active', when((col('active') == 1), 1).otherwise(0)) \
    .withColumn('non-active', when((col('active') == 0), 1).otherwise(0)) \
    .groupby('city') \
    .agg(sum(col('active')), sum(col('non-active'))) \
    .sort(col('sum(non-active)').desc()) \
    .show() 

# task7

print("Output for 7 task")

rental_time_table = rental.join(customer, rental.customer_id == customer.customer_id, 'inner') \
	  .join(address, address.address_id == customer.address_id, 'inner') \
	  .join(city, city.city_id == address.city_id, 'inner') \
	  .join(inventory, rental.inventory_id == inventory.inventory_id, 'inner') \
	  .join(film_category, inventory.film_id == film_category.film_id, 'right') \
	  .join(category, film_category.category_id == category.category_id, 'inner') \
	  .join(film, film.film_id == film_category.film_id, 'inner') \
	  .withColumn('time in city with a_ and A_', when((col('city').like('a%') | col('city').like('A%')), col('rental_duration')).otherwise(0)) \
	  .withColumn('time in city with _-_', when((col('city').like("%-%")), col('rental_duration')).otherwise(0)) \

rental_time_table.groupby('name').sum('time in city with a_ and A_').sort(col('sum(time in city with a_ and A_)').desc()).show(1)

rental_time_table.groupby('name').sum('time in city with _-_').sort(col('sum(time in city with _-_)').desc()).show(1)
