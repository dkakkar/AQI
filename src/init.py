#This script creates the station, weather, daily and hourly tables in PostGIS
#Author:Devika Kakkar
#Updated:Sept 2,2016

import psycopg2

def main():
        #print("running")
        #DB connection
        conn=psycopg2.connect("dbname=postgres user=postgres host=localhost port=5434 password=postgres")
        cur=conn.cursor()
        #Create tables
        cur.execute("CREATE TABLE stations(count integer, date character varying, id integer, name character varying, latitude double precision, longitude double precision, stateID integer, cityID integer, live character varying, flag character varying) WITH (OIDS=FALSE); ALTER TABLE public.stations OWNER TO postgres;")
        cur.execute("CREATE TABLE weather(count integer, date character varying, hour character varying, id integer, name character varying, humidity integer, temperature double precision, pressure integer, windspeed double precision) WITH (OIDS=FALSE); ALTER TABLE public.stations OWNER TO postgres;")
        cur.execute("CREATE TABLE daily(count integer, date character varying, hour character varying, id integer, name character varying, PM10_avg character varying,PM10_min character varying, PM10_max character varying, SO2_avg character varying, SO2_min character varying,SO2_max character varying,NO2_avg character varying, NO2_min character varying,NO2_max character varying,PM25_avg character varying, PM25_min character varying,PM25_max character varying,CO_avg character varying, CO_min character varying,CO_max character varying,O3_avg character varying, O3_min character varying,O3_max character varying, status character varying) WITH (OIDS=FALSE); ALTER TABLE public.stations OWNER TO postgres;")
        cur.execute("CREATE TABLE hourly(count integer, date character varying, hour character varying, id integer, name character varying, PM10 character varying, SO2 character varying,NO2 character varying,PM25 character varying,CO character varying,O3 character varying, status character varying) WITH (OIDS=FALSE); ALTER TABLE public.stations OWNER TO postgres;")
        conn.commit()
        

main()
 

