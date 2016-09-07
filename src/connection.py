#This script connects to the DB
#Author:Devika Kakkar
#Updated:Sept 2,2016


import psycopg2


def connect():
        print("Connecting")
        #Connect to DB, locations table made by init
        conn=psycopg2.connect("dbname=postgres user=postgres host=localhost port=5434 password=postgres")
        cur=conn.cursor()
        print("Connected")
        return cur,conn

        




