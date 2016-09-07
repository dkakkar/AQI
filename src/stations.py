#This script updates the stations tables and calls the daily and hourly module
#Author:Devika Kakkar
#Updated:Sept 2,2016

import urllib.parse
import urllib.request
import json
import sys
import csv
import schedule
import time
import psycopg2
import daily
import hourly
from datetime import date
import connection

def main():
        print("Running stations")
        #Connect to DB, locations table made by init
        cur,conn=connection.connect()
        c=0
        #Parse json form url       
        url = 'http://164.100.160.234:9000/stations'
        status = 'C'
        response = urllib.request.urlopen(url)
        the_page = response.read()
        response.close
        json_str = the_page.decode()
        parsed_data = json.loads(json_str)
        lyst_station = parsed_data['stations']
        for station in lyst_station:
                lyst_stationInCity =station['stationsInCity']                       
                for stationInCity in lyst_stationInCity:
                            sql = "Select name, latitude, longitude from stations where id =" + str(stationInCity['id']) + "and flag = " + repr('C')
                            cur.execute(sql)
                            rows=cur.fetchall()
                            day_strftime = time.strftime("%d/%m/%Y")
                            lyst_strftime= day_strftime.split('/')
                            #print(lyst_strftime)
                            day=date(int(lyst_strftime[2]),int(lyst_strftime[1]),int(lyst_strftime[0])).isoformat()
                            
                            
                            #If id not found in table, insert it 
                            if(rows == []):
                                   #print("station does not exists, updating table")
                                    c = c+1
                                    cur.execute("Insert into stations Values (%d,%s,%d,%s,%.5f,%.5f,%d,%d,%s,%s);" % (c,repr(day),(stationInCity['id']),(repr(stationInCity['name'])),stationInCity['latitude'],
                                                                                                       stationInCity['longitude'],stationInCity['stateID'],stationInCity['cityID'],stationInCity['live'],(repr(status))))                                                            

                            #Else check for name, lat/long (if same, skip) else insert it in table and set old flag to "D"
                            else:
                                    #print("id exists, checking name/lat/long")
                                    res_name = [x[0] for x in rows]
                                    res_lat = [x[1] for x in rows]
                                    res_lon= [x[2] for x in rows]
                                    if((res_name[0] == stationInCity['name']) and (res_lat[0] == round(stationInCity['latitude'],5)) and (res_lon[0] == round(stationInCity['longitude'],5)) ):
                                            print("Station exists, name/lat/long same")
                                    else:
                                            #print("Station changed,updating flag and inserting")
                                            sql = "Update stations SET flag = 'D' where id =" + str(stationInCity['id']);
                                            cur.execute(sql)
                                            cur.execute("Select count(*) from stations;")
                                            len_tab = cur.fetchall()
                                            res_len = [x[0] for x in len_tab]
                                            c= (res_len[0]+1)
                                            cur.execute("Insert into stations Values (%d,%s,%d,%s,%.5f,%.5f,%d,%d,%s,%s);" % (c,repr(day),(stationInCity['id']),(repr(stationInCity['name'])),stationInCity['latitude'],
                                                                                                       stationInCity['longitude'],stationInCity['stateID'],stationInCity['cityID'],stationInCity['live'],(repr(status))))
                                            
                                   
        #Commit latest stations table to DB       
        conn.commit()                          
        #Writing to CSV                
        cur.execute("COPY stations(count, date,id,name,latitude,longitude,stateid,cityid,live,flag) TO '/tmp/stations.csv' DELIMITER ',' CSV HEADER;")
        #Call daily table module
        daily.main()
        #Call hourly table module
        hourly.main()
        print("Ending stations")
                            
main()

##Schedule the module to run everydy at 23
 
schedule.every().day.at('23:00').do(main)

while True:
    schedule.run_pending()
    time.sleep(1)


