import pandas as pd
import time
import os
import asyncio
import logging


real_time_events_1={}
real_time_events_2={}
scale = 10**3 #scale 1 second to 1 millisecond, less the scale, more will be accurate output.
#scale = 10**2 #scale 1 second to 10 milliseconds, but run slow to simulate

stop_event = asyncio.Event()

async def exceed12hrOnline():
    i=0
    while not stop_event.is_set():
        remove = []
        for driver_id, event in real_time_events_2.items():
            if event['total_online_time'] > (12*60*60)/scale:
                i+=1
                alert_msg = f"> Alert A {i}: Driver {event['name']} exceeded 12 hours online in total"
                print(alert_msg)
                logging.info(alert_msg)
                remove.append(driver_id)

        for driver_id in remove:
            del real_time_events_2[driver_id]

        await asyncio.sleep(1/scale)
    

async def alertIfOnline30min():
    i=0
    while not stop_event.is_set():
        now=time.time()
        remove = []
        for driver_id, event in real_time_events_1.items():
            if event['status'] == 'DRIVER_STATUS_ONLINE' and now - event['updated_at'] > (30*60)/scale:
                i+=1
                alert_msg = f"# Alert B {i}: Driver {event['name']} is online for more than 30 minutes"
                print(alert_msg)
                logging.info(alert_msg)
                remove.append(driver_id)
                

        for driver_id in remove:
            del real_time_events_1[driver_id]

        await asyncio.sleep(1/scale)        

#simulating webhook service events
def add_events(event):
    now = time.time()
    real_time_events_1[event["driver_id"]] = { 'status':event["status"],'updated_at':now,'name':str(event['first_name'])+" "+str(event['last_name'])}
    

    if event['driver_id'] not in real_time_events_2:
            real_time_events_2[event["driver_id"]] = {'status':event["status"],"updated_at":now,"total_online_time":0,"name":str(event['first_name'])+" "+str(event['last_name'])}
    else:
        if real_time_events_2[event["driver_id"]]['status'] == 'DRIVER_STATUS_ONLINE':
            real_time_events_2[event["driver_id"]]['total_online_time']+=now-real_time_events_2[event["driver_id"]]['updated_at']

        real_time_events_2[event["driver_id"]]['updated_at']=now
        real_time_events_2[event["driver_id"]]['status']=event['status']

    


async def read_excel():
    file_path = "Webhook_Events_02Aug_to_03Aug_2025.xlsx"
    df = pd.read_excel(file_path)
    #df["unix_timestamp"] = df["event_time"].astype(int)// 10**9 #in secs from epoch
    #print(df['status'].unique())
    
    total_events = len(df)
    delay_time = [(df["event_time"][i]-df["event_time"][i-1]).total_seconds() for i in range(1,total_events)]
    #print(delay_time[0])
      
    for index, row in df.iterrows():        
        #print(f"Events {index + 1}")
        #print(row.to_dict())
        add_events(row.to_dict())
        #simulate real time events
        if index < total_events-1:
            await asyncio.sleep(delay_time[index]/scale)
    stop_event.set()

async def main():
    
    logging.basicConfig(
        filename='alerts.log',
        level=logging.INFO,
        format='%(asctime)s - %(message)s'
    )
    
    await asyncio.gather(
        read_excel(),
        alertIfOnline30min(),
        exceed12hrOnline()
    )

if __name__ == "__main__":
    asyncio.run(main())

#python3 ./simulate.py