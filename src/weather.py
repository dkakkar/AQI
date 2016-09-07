#This script updates the weather table
#Author:Devika Kakkar
#Updated:Sept 2,2016

import urllib.parse
import urllib.request
import json
import schedule
import time
import psycopg2
from datetime import date
import connection



def main():
        #Connect to DB, locations table made by init
        print("running weather")
        cur,conn=connection.connect()
        day = time.strftime("%d/%m/%Y")
        hr = time.strftime("%H")
        lyst_strftime= day.split('/')
                            #print(lyst_strftime)
        day_nr=date(int(lyst_strftime[2]),int(lyst_strftime[1]),int(lyst_strftime[0])).isoformat()
                            

        count=0
        #Parse json form url
        url = 'http://api.openweathermap.org/data/2.5/weather'
        sql = "Select distinct id from stations;" 
        cur.execute(sql)
        rows=cur.fetchall()
        res_list = [x[0] for x in rows]
        for i in range(len(rows)):
                sql = "Select name from stations where id =" + str(res_list[i])  + "and flag = " + repr('C')
                cur.execute(sql)
                name_rows =cur.fetchall()
                res_name = [x[0] for x in name_rows]
                
                #Latitude
                sql = "Select latitude from stations where id =" + str(res_list[i])  + "and flag = " + repr('C')
                cur.execute(sql)
                lats_rows =cur.fetchall()
                res_lat = [x[0] for x in lats_rows]
                
               # Long
                sql = "Select longitude from stations where id =" + str(res_list[i]) + "and flag = " + repr('C')
                cur.execute(sql)
                lon_rows=cur.fetchall()
                res_lon = [x[0] for x in lon_rows]
                values = { }
                values['units'] = 'imperial'
                values['appid']= '5a7d225dd6ec1798a6919a49eb416346'
                values['lat'] = res_lat[0]
                values['lon']= res_lon[0]
                data= urllib.parse.urlencode(values)
                full_url = url + "?"+ data
                response = urllib.request.urlopen(full_url)
                the_page = response.read()
                response.close
                json_str = the_page.decode()
                parsed_data = json.loads(json_str)
                humidity_val= parsed_data["main"]["humidity"]
                temp_val= parsed_data["main"]["temp"]
                pressure_val= parsed_data["main"]["pressure"]
                wind_val= parsed_data["wind"]["speed"]
                cur.execute("Select count(*) from weather;")
                len_tab = cur.fetchall()
                res_len = [x[0] for x in len_tab]
                count= (res_len[0]+1)
                cur.execute("Insert into weather Values (%d,%50s,%10s,%d,%50s,%d,%.5f,%d,%.5f);" % (count,repr(day_nr),repr(hr),(res_list[i]),repr(res_name[0]),humidity_val,
                                                                                                               temp_val,pressure_val,wind_val))                                                            
        #Commit to DB
        conn.commit()
        #Copy to csv
        cur.execute("COPY weather(count, date,hour ,id,name,humidity,temperature,pressure,windspeed) TO '/tmp/weather.csv' DELIMITER ',' CSV HEADER;")
        print("Ending weather")
                                                 
                            
main()

schedule.every(1).hour.do(main)

while True:
    schedule.run_pending()
    time.sleep(1)

