#This script updates the hourly table
#Author:Devika Kakkar
#Updated:Sept 2,2016

import urllib.parse
import urllib.request
import json
import schedule
import time
import psycopg2

def main():
        print("running hourly")
        #Connect to DB, locations table made by init
        conn=psycopg2.connect("dbname=postgres user=postgres host=localhost port=5434 password=postgres")
        cur=conn.cursor()
        day_url = time.strftime("%d/%m/%Y")
        c=0
        url_1 = 'http://164.100.160.234:9000/metrics/station'
        sql = "Select distinct id from stations;" 
        cur.execute(sql)
        rows=cur.fetchall()
        res_list = [x[0] for x in rows]
        for id_v in range(len(rows)):
                sql = "Select name from stations where id =" + str(res_list[id_v])  + "and flag = " + repr('C')
                cur.execute(sql)
                name_rows =cur.fetchall()
                res_name = [x[0] for x in name_rows]
                #Send url request
                url= url_1+ '/'+ str(res_list[id_v])
                values = { }
                values['d'] = day_url
                values['h']= '23'
                data= urllib.parse.urlencode(values)
                full_url = url + "?"+ data
                #print(full_url)
                response = urllib.request.urlopen(full_url)
                the_page = response.read()
                response.close
                json_str = the_page.decode()
                a = json.loads(json_str)
                # Insert values in hourly table
                if (a["metrics"]==[]):
                       #print("Station Not reporting")
                       stat ='NR'
                       cur.execute("Select count(*) from hourly;")
                       len_tab = cur.fetchall()
                       res_len = [x[0] for x in len_tab]
                       c= (res_len[0]+1)
                       day_nr=time.strftime("%B %d, %Y")
                       cur.execute("Insert into hourly Values (%d,%50s,%10s,%d,%50s,%s,%s,%s,%s,%s,%s,%s);" % (c,repr(day_nr),repr('N/A'),(res_list[id_v]),repr(res_name[0]),repr('N/A'), repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('N/A'),repr('NR')))
                       
                else:
                        pollutants = [a['metrics'][i]['name'] for i in range(len(a['metrics']))]
                        dates = [a['metrics'][i]['data'][j]['date'] for i in range(len(a['metrics'])) for j in range(len(a['metrics'][i]['data']))]                              
                        result = {date:{pollutant:'NA'for pollutant in pollutants} for date in dates}
                        for i in range(len(a['metrics'])):
                            for j in range(len(a['metrics'][i]['data'])): 
                                result[a['metrics'][i]['data'][j]['date']][a['metrics'][i]['name']] = a['metrics'][i]['data'][j]['value']
                               
                        for key_date, value in result.items():
                                    stat='R'
                                    lyst_date= key_date.split()
                                    day=lyst_date[0]+" "+lyst_date[1]+lyst_date[2]
                                    hr=lyst_date[3]
                                    try:
                                            PM10_v= value['PM10']
                                    except:
                                            PM10_v='N/A'
                                    try:
                                            SO2_v= value['SO2']
                                    except:
                                            SO2_v='N/A'
                                    try:
                                            PM25_v= value['PM2.5']
                                    except:
                                            PM25_v='N/A'
                                    try:
                                            CO_v= value['CO']
                                    except:
                                            CO_v='N/A'
                                    try:
                                            O3_v= value['O3']
                                    except:
                                            O3_v= 'N/A'
                                    try:
                                            NO2_v= value['NO2']
                                    except:
                                            NO2_v='N/A'

                                    cur.execute("Select count(*) from hourly;")
                                    len_tab = cur.fetchall()
                                    res_len = [x[0] for x in len_tab]
                                    c= (res_len[0]+1)                                           
                                    cur.execute("Insert into hourly Values (%d,%50s,%10s,%d,%50s,%s,%s,%s,%s,%s,%s,%s);" % (c,repr(day),repr(hr),(res_list[id_v]),repr(res_name[0]),repr(PM10_v),repr(SO2_v),repr(NO2_v),repr(PM25_v),repr(CO_v),repr(O3_v),repr(stat)))
                #Commit changes to DB
                conn.commit()
                #Write to csv
                cur.execute("COPY hourly(count,date,hour,id,name,pm10,so2,no2,pm25,co,o3,status) TO '/tmp/hourly.csv' DELIMITER ',' CSV HEADER;")
        print("Ending hourly")
                
                                 
                            
if __name__ == "__main__":
   # stuff only to run when not called via 'import' here
   main()
 

