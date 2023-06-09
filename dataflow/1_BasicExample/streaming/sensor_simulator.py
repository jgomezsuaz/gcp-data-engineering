import csv
import json
import logging
import threading
import time
import argparse
import concurrent.futures
from google.cloud import pubsub_v1

def csv_reader(sensor, dataLocation, multiple):
    with open(dataLocation, newline='') as File:
        reader = csv.reader(File)
        next(reader)
        filtered = filter(lambda x: x[1] == sensor, reader) if multiple else reader
        for row in filtered:
            yield row

            
class PubSubThread:
    def __init__(self, publisher, topic_path, dataLocation, dataPerMinute):
        self.publisher = publisher
        self.topic_path = topic_path
        self.dataLocation = dataLocation
        self.sleepingTime =  60 / dataPerMinute

    def thread_function(self, sensor):
        logging.info("Sensor %s: starting", sensor)
        for row in csv_reader(sensor, self.dataLocation, False):
            body = {
                "sensor": row[1],
                "timestamp": int(round(float(row[0]) * 1000)),
                "co": float(row[2]),
                "humidity": float(row[3]),
                "light": row[4].lower() in ("yes", "true", "t", "1"),
                "lpg": float(row[5]),
                "motion": row[6].lower() in ("yes", "true", "t", "1"),
                "smoke": float(row[7]),
                "temp": float(row[8])
            }
            data = json.dumps(body)
            self.publisher.publish(self.topic_path, data.encode("utf-8"))
            time.sleep(self.sleepingTime)
        logging.info("Sensor %s: finishing.", sensor)
        
    

def run():
    # Command line arguments
    parser = argparse.ArgumentParser(description='Run a IoT sensor and send data to Pubsub')
    parser.add_argument('--project',required=True, help='Specify Google Cloud project')
    parser.add_argument('--topic', required=True, help='Specify Google Cloud Pubsub topic')
    parser.add_argument('--dataLocation', required=True, help='Specify location for de CSV file')
    parser.add_argument('--dataPerMinute', type=int, required=True, help='Data per minute per sensor')

    opts = parser.parse_args()
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(opts.project, opts.topic)
    
    sensors = ['b8:27:eb:bf:9d:51', '00:0f:00:70:91:0a', '1c:bf:ce:15:ec:4d']
    
    pubsub_thread = PubSubThread(publisher, topic_path, opts.dataLocation, opts.dataPerMinute)

    # TODO: Revisar porque no se ejecuta al usar hilos
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(pubsub_thread.thread_function, sensors)
    """
    
    pubsub_thread.thread_function("global")
        
        
if __name__ == '__main__':
    print('Working...')
    run()
