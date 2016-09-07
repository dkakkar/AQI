#This script updates the daily table
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
        #Connect to DB, locations table
        print("Running daily")
        cur,conn=connection.connect()
        day = time.strftime("%d/%m/%Y")
        hr='23'
        count=0    
        url_1 = 'http://164.100.160.234:9000/metrics/station'
        sql = "Select distinct id from stations;" 
        cur.execute(sql)
        rows=cur.fetchall()
        res_list = [x[0] for x in rows]
        for i in range(len(rows)):
                sql = "Select name from stations where id =" + str(res_list[i])  + "and flag = " + repr('C')
                cur.execute(sql)
                name_rows =cur.fetchall()
                res_name = [x[0] for x in name_rows]
                #create url for daily reading
                url= url_1+ '/'+ str(res_list[i])
                values = { }
                values['d'] = day
                values['h']= '23'
                data= urllib.parse.urlencode(values)
                full_url = url + "?"+ data
                response = urllib.request.urlopen(full_url)
                the_page = response.read()
                response.close
                json_str = the_page.decode()
                parsed_data = json.loads(json_str)
                lyst_strftime= day.split('/')
                       #print(lyst_strftime)
                day_nr=date(int(lyst_strftime[2]),int(lyst_strftime[1]),int(lyst_strftime[0])).isoformat()
                            

               # Insert values in table
                if (parsed_data["metrics"]==[]):
                       #print("Station Not reporting")
                       stat ='NR'
                       cur.execute("Select count(*) from daily;")
                       len_tab = cur.fetchall()
                       res_len = [x[0] for x in len_tab]
                       #print(res_len)
                       count= (res_len[0]+1)                     
                       cur.execute("Insert into daily Values (%d,%50s,%10s,%d,%50s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" % (count,repr(day_nr),repr(hr),(res_list[i]),repr(res_name[0]),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr(stat)))                  
                else:
                       # print("Station reporting")
                        j=0
                        lyst_name=[]
                        stat='R'
                        cur.execute("Select count(*) from daily;")
                        len_tab = cur.fetchall()
                        res_len = [x[0] for x in len_tab]
                        count= (res_len[0]+1)
                        #day_nr=time.strftime("%B %d, %Y")
                        l= len(parsed_data["metrics"])
                        #Find the list of pollutants present
                        while (j<l):
                                lyst_name.append(parsed_data["metrics"][j]["name"])
                                j=j+1
                        #PM10
                        if ("PM10" in lyst_name):
                                c= lyst_name.index("PM10")
                                PM10_avg_v = parsed_data["metrics"][c]["avg"]
                                PM10_min_v = parsed_data["metrics"][c]["min"]
                                PM10_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("PM10 not found")
                                PM10_avg_v = 'N/A'
                                PM10_min_v = 'N/A'
                                PM10_max_v = 'N/A'

                        #SO2
                        if ("SO2" in lyst_name):
                                c= lyst_name.index("SO2")
                                SO2_avg_v = parsed_data["metrics"][c]["avg"]
                                SO2_min_v = parsed_data["metrics"][c]["min"]
                                SO2_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("SO2 not found")
                                SO2_avg_v = 'N/A'
                                SO2_min_v = 'N/A'
                                SO2_max_v = 'N/A'

                        #NO2
                        if ("NO2" in lyst_name):
                                c= lyst_name.index("NO2")
                                NO2_avg_v = parsed_data["metrics"][c]["avg"]
                                NO2_min_v = parsed_data["metrics"][c]["min"]
                                NO2_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("NO2 not found")
                                NO2_avg_v = 'N/A'
                                NO2_min_v = 'N/A'
                                NO2_max_v = 'N/A'

                        #PM2.5
                        if ("PM2.5" in lyst_name):
                                c= lyst_name.index("PM2.5")
                                PM25_avg_v = parsed_data["metrics"][c]["avg"]
                                PM25_min_v = parsed_data["metrics"][c]["min"]
                                PM25_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("PM2.5 not found")
                                NO2_avg_v = 'N/A'
                                NO2_min_v = 'N/A'
                                NO2_max_v = 'N/A'

                        #CO
                        if ("CO" in lyst_name):
                                c= lyst_name.index("CO")
                                CO_avg_v = parsed_data["metrics"][c]["avg"]
                                CO_min_v = parsed_data["metrics"][c]["min"]
                                CO_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("CO not found")
                                CO_avg_v = 'N/A'
                                CO_min_v = 'N/A'
                                CO_max_v = 'N/A'


                        #O3
                        if ("O3" in lyst_name):
                                c= lyst_name.index("O3")
                                O3_avg_v = parsed_data["metrics"][c]["avg"]
                                O3_min_v = parsed_data["metrics"][c]["min"]
                                O3_max_v = parsed_data["metrics"][c]["max"]
                        else:
                                #print("O3 not found")
                                O3_avg_v = 'N/A'
                                O3_min_v = 'N/A'
                                O3_max_v = 'N/A'
                        
                        cur.execute("Insert into daily Values (%d,%50s,%10s,%d,%50s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" % (count,repr(day_nr),repr(hr),(res_list[i]),repr(res_name[0]),repr(PM10_avg_v),repr(PM10_min_v),repr(PM10_max_v),repr(SO2_avg_v),repr(SO2_min_v),repr(SO2_max_v), repr(NO2_avg_v),repr(NO2_min_v),repr(NO2_max_v),repr(PM25_avg_v),repr(PM25_min_v),repr(PM25_max_v),repr(CO_avg_v),repr(CO_min_v),repr(CO_max_v),repr(O3_avg_v),repr(O3_min_v),repr(O3_max_v),repr(stat)))
 
                       
        #Commit changes to DB
        conn.commit()
        #Write to csv
        cur.execute("COPY daily(count, date,hour ,id,name,pm10_avg,pm10_min,pm10_max,so2_avg,so2_min,so2_max,no2_avg,no2_min,no2_max,pm25_avg,pm25_min,pm25_max,co_avg,co_min,co_max,o3_avg,o3_min,o3_max,status) TO '/tmp/daily.csv' DELIMITER ',' CSV HEADER;")
        print("Ending daily")
                                                 
                            
if __name__ == "__main__":
   main()


