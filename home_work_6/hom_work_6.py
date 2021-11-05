#!/usr/bin/env python
# coding: utf-8

# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import when
from pyspark.sql.functions import col, lag, unix_timestamp
#from pyspark.sql.functions import unix_timestamp
import datetime
import pyspark.sql.functions as F

import psycopg2
#from hdfs import InsecureClient

import os
from datetime import datetime


spark = SparkSession.builder    .config('spark.driver.extraClassPath'
            , '/home/user/shared_folder/postgresql-42.3.1.jar')\
    .master('local')\
    .appName("home_work 13")\
    .getOrCreate()


jdbcDF = spark.read     .format("jdbc")     .option("url", "jdbc:postgresql://192.168.77.208:5432/dshop_bu")     .option("dbtable", "film_category")     .option("user", "pguser")     .option("password", "secret")     .load()


pg_url = "jdbc:postgresql://192.168.77.208:5432/dshop_bu"
pg_properties = {"user": "pguser", "password": "secret"}


film_category_df = spark.read.jdbc(pg_url
                     , table='film_category'
                     , properties=pg_properties)

category_df = spark.read.jdbc(pg_url
                     , table='category'
                     , properties=pg_properties)

rental_df = spark.read.jdbc(pg_url
                     , table='rental'
                     , properties=pg_properties)

inventory_df = spark.read.jdbc(pg_url
                     , table='inventory'
                     , properties=pg_properties)

film_actor_df = spark.read.jdbc(pg_url
                     , table='film_actor'
                     , properties=pg_properties)

actor_df = spark.read.jdbc(pg_url
                     , table='actor'
                     , properties=pg_properties)


payment_df = spark.read.jdbc(pg_url
                     , table='payment'
                     , properties=pg_properties)

film_df = spark.read.jdbc(pg_url
                     , table='film'
                     , properties=pg_properties)

customer_df = spark.read.jdbc(pg_url
                     , table='customer'
                     , properties=pg_properties)


address_df = spark.read.jdbc(pg_url
                     , table='address'
                     , properties=pg_properties)


city_df = spark.read.jdbc(pg_url
                     , table='city'
                     , properties=pg_properties)



#1
res1 = film_category_df    .join(category_df, film_category_df.category_id == category_df.category_id)    .select(film_category_df.category_id
            , category_df.name)


res1.groupby(res1.category_id, res1.name).count().sort(F.desc('count')).show()


#2
join_df = rental_df    .join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id)    .select(inventory_df.film_id
            , rental_df.rental_id)


f_df=join_df.groupby(join_df.film_id).count().withColumnRenamed("count", "cnt_rent")


res_df=f_df        .join(film_actor_df, f_df.film_id == film_actor_df.film_id)        .join(actor_df, film_actor_df.actor_id == actor_df.actor_id)        .select(actor_df.actor_id
            , actor_df.first_name
            , F.concat(
                            actor_df.first_name
                            , F.lit(' ')
                            ,actor_df.last_name
                        ).alias("fullname")
            , f_df.cnt_rent)


res_df.groupby(res_df.actor_id, res_df.fullname).sum("cnt_rent").sort(F.desc('sum(cnt_rent)')).show()

#3
sub_df=film_category_df        .join(category_df, film_category_df.category_id == category_df.category_id)        .join(inventory_df, film_category_df.film_id == inventory_df.film_id)        .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)        .join(payment_df, rental_df.rental_id == payment_df.rental_id)        .select(category_df.category_id
            , category_df.name
            , payment_df.amount)


sub_agg_df=sub_df.groupby(sub_df.category_id, sub_df.name).sum("amount").withColumnRenamed("sum(amount)", "sum_amount").sort(F.desc('sum_amount'))

sub_agg_df.groupby(sub_agg_df.category_id, sub_agg_df.name).sum("sum_amount").sort(F.desc('sum(sum_amount)')).limit(1).show()


#4
join_df=film_category_df        .join(film_df, film_category_df.film_id == film_df.film_id)        .join(inventory_df, inventory_df.film_id == film_category_df.film_id,"left")        .filter(inventory_df.film_id.isNull()).select(film_df.film_id
            , film_df.title).show()


#5
sub5_df=film_category_df        .join(category_df, film_category_df.category_id == category_df.category_id)        .join(film_actor_df, film_category_df.film_id == film_actor_df.film_id)        .join(actor_df, film_actor_df.actor_id == actor_df.actor_id)        .filter(category_df.name == "Children").select(actor_df.actor_id
            , actor_df.first_name
            , F.concat(
                            actor_df.first_name
                            , F.lit(' ')
                            ,actor_df.last_name
                        ).alias("fullname")
            , film_category_df.film_id)


sub5_agg_df=sub5_df.groupby(sub5_df.actor_id, sub5_df.fullname).agg(countDistinct("film_id")).withColumnRenamed("count(film_id)", "cnt_films")


sub5_join_df=sub5_agg_df.select(sub5_agg_df.actor_id, sub5_agg_df.cnt_films).sort(F.desc('cnt_films')).limit(3)

sub5_agg_df        .join(sub5_join_df, sub5_agg_df.cnt_films == sub5_join_df.cnt_films)        .select(sub5_agg_df.actor_id, sub5_agg_df.fullname, sub5_agg_df.cnt_films)        .distinct()        .show()



#6
sub6_df=customer_df        .join(address_df, customer_df.address_id == address_df.address_id)        .join(city_df, address_df.city_id == city_df.city_id)        .select(city_df.city_id
                        , customer_df.customer_id
                        , when(customer_df.active==1,'активный')
                        .when(customer_df.active!=1,'не активный')
                        .otherwise(customer_df.active).alias("active"))

sub6_df.groupby(sub6_df.city_id, sub6_df.active).agg(countDistinct("customer_id")).sort(F.asc('active'), F.desc('count(customer_id)')).show()

rental_df.select(rental_df.rental_id                 , rental_df.return_date                 , rental_df.rental_date                 , F.hour(rental_df.return_date)-F.hour(rental_df.rental_date)).show()


#7
sub7_df=rental_df        .join(customer_df, rental_df.customer_id == customer_df.customer_id)        .join(address_df, customer_df.address_id == address_df.address_id)        .join(city_df, address_df.city_id == city_df.city_id)        .join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id)        .join(film_category_df, inventory_df.film_id == film_category_df.film_id)        .join(film_df, film_category_df.film_id == film_df.film_id)        .join(category_df, film_category_df.category_id == category_df.category_id)        .filter(film_df.title.like("A%"))        .select(category_df.category_id                , category_df.name                , ((unix_timestamp(rental_df.return_date)-unix_timestamp(rental_df.rental_date)).cast('float')/3600).alias("dt_diff")
                        )

cols = ['category_id', 'name', 'sum(dt_diff)']
sub7_agg_df=sub7_df.groupby(sub7_df.category_id, sub7_df.name).sum("dt_diff").sort(F.desc('sum(dt_diff)')).limit(1).select(cols)


sub7_c_df=rental_df        .join(customer_df, rental_df.customer_id == customer_df.customer_id)        .join(address_df, customer_df.address_id == address_df.address_id)        .join(city_df, address_df.city_id == city_df.city_id)        .join(inventory_df, rental_df.inventory_id == inventory_df.inventory_id)        .join(film_category_df, inventory_df.film_id == film_category_df.film_id)        .join(film_df, film_category_df.film_id == film_df.film_id)        .join(category_df, film_category_df.category_id == category_df.category_id)        .filter(city_df.city.like("%-%"))        .select(category_df.category_id                , category_df.name                , ((unix_timestamp(rental_df.return_date)-unix_timestamp(rental_df.rental_date)).cast('float')/3600).alias("dt_diff")
                        )

sub7_agg_c_df=sub7_c_df.groupby(sub7_c_df.category_id, sub7_c_df.name).sum("dt_diff").sort(F.desc('sum(dt_diff)')).limit(1).select(cols)


r=sub7_agg_df.union(sub7_agg_c_df)


r.show()

