""""
Levi Lowther
27May2024
Producer of messages for Heart Rate Telemetry System

""" 
import pika
import sys
import webbrowser
import csv
import struct
import time 
from datetime import datetime
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# use to control whether or not admin page is offered to user.
# change to true to receive offer, false to remove prompt
show_offer = False

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer:
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()


def main():
    """
    Read a CVS, row by row. Send messages based on the queue the info is coming from 
    """
   
    offer_rabbitmq_admin_site()

    logger.info(f'Attempting to access telemetry.csv')

    try:
        # access file
        with open("telemetry.csv", newline='') as csvfile:
            reader = csv.reader(csvfile)

            # Skip header row
            next(reader)

            # Main section of code where data is reviewed and messages are sent.
            for row in reader:

                # assign variables from row
                index = row[0]
                string_timestamp = row[1]
                patient_1 = row[2]
                patient_2 = row[3]
                patient_3 = row[4]
                patient_4 = row[5]
                patient_5 = row[6]

                               
                # convert datetime string into a datetime object
                datetime_timestamp = datetime.strptime(string_timestamp, "%Y-%m-%d %H:%M:%S.%f").timestamp()
                
                #check for value and send to correct queue
                if patient_1:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(patient_1))
                    send_message('localhost', 'Patient_1', message)

                # Patient 2
                if patient_2:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(patient_2))
                    send_message('localhost', 'Patient_2', message)

                # Patient 3
                if patient_3:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(patient_3))
                    send_message('localhost', 'Patient_3', message)

                if patient_4:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(patient_4))
                    send_message('localhost', 'Patient_4', message)

                if patient_5:
                    # pack message contents into serialized format
                    message = struct.pack("!df", datetime_timestamp, float(patient_5))
                    send_message('localhost', 'Patient_5', message)
                time.sleep(.05)

                
    except Exception as e:
        logger.info(f'ERROR: {e}')

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

       """

    try:
        logger.info(f"send_message({host=}, {queue_name=}, {message=})")
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))

        # use the connection to create a communication channel
        ch = conn.channel()
        logger.info(f"connection opened: {host=}, {queue_name=}")

        # declare a durable queue
        ch.queue_declare(queue=queue_name, durable=True)

        #publish a message to the queue
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)

        # print a message to the console for the user
        logger.info(f" [x] Sent {message}")

    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
        
    finally:
        # close the connection to the server
        conn.close()
        logger.info(f"connection closed: {host=}, {queue_name=}")
        

# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # specify file path for data source
    file_path = 'telemetry.csv'

    # transmit task list
    logger.info(f'Beginning process: {__name__}')
    main()