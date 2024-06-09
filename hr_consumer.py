""""
Levi Lowther
03June2024
Consumer of messages for Heart Rate Telemetry Monitoring System
""" 
import pika
import sys
import time
import struct
from datetime import datetime
from collections import deque
from util_logger import setup_logger

logger, logname = setup_logger(__file__)

# create deques with a max length of 3
patient1 = deque(maxlen = 3) # 1.5 min * 1 reading/0.5 min
patient2 = deque(maxlen = 3) # 1.5 min * 1 reading/0.5 min
patient3 = deque(maxlen = 3) # 1.5 min * 1 reading/0.5 min
patient4 = deque(maxlen = 3) # 1.5 min * 1 reading/0.5 min
patient5 = deque(maxlen = 3) # 1.5 min * 1 reading/0.5 min


# define callbacks for each queue when called

def p1_callback(ch, method, properties, body):
    """ 
    Patient 1 queue callback function
    """
    # unpack struct of hr_producer
    timestamp, heartrate = struct.unpack('!df', body) 

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S.%f")
    logger.info(f' [Patient 1 Current Heart Rate]: {heartrate} bpm : {timestamp_str}')

    # Add new heartrate
    patient1.append(heartrate)

    # check heart rate once deque is at maxlen
    if len(patient1) == patient1.maxlen:
        # check for too high heart rate and alert if all values in deque are over threshold
        if all(value > 100 for value in patient1) == True:
            logger.info(f'''
Patient 1 Tachycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate Time of Alert: {heartrate}''')
        #check for too low heart rate
        if all(value < 50 for value in patient1) == True:
            logger.info(f'''
Patient 1 Bradycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
      
    ch.basic_ack(delivery_tag=method.delivery_tag)


def p2_callback(ch, method, properties, body):
    """ 
    Patient 2 queue callback function
    """
    # unpack struct of hr_producer
    timestamp, heartrate = struct.unpack('!df', body) 

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S.%f")
    logger.info(f' [Patient 2 Current Heart Rate]: {heartrate} bpm : {timestamp_str}')

    # Add new heartrate
    patient2.append(heartrate)

    # check heart rate deque is at maxlen
    if len(patient2) == patient2.maxlen:
        # check for too high heart rate and alert if all values in deque are over threshold
        if all(value > 100 for value in patient2) == True:
            logger.info(f'''
Patient 2 Tachycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
        #check for too low heart rate
        if all(value < 50 for value in patient2) == True:
            logger.info(f'''
Patient 2 Bradycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
      
    ch.basic_ack(delivery_tag=method.delivery_tag)

def p3_callback(ch, method, properties, body):
    """ 
    Patient 3 queue callback function
    """
    # unpack struct of hr_producer
    timestamp, heartrate = struct.unpack('!df', body) 

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S.%f")
    logger.info(f' [Patient 3 Current Heart Rate]: {heartrate} bpm : {timestamp_str}')

    # Add new heartrate
    patient3.append(heartrate)

    # check for heart rate once deque is at maxlen
    if len(patient3) == patient3.maxlen:
        # check for too high heart rate and alert if all values in deque are over threshold
        if all(value > 100 for value in patient3) == True:
            logger.info(f'''
Patient 3 Tachycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
        #check for too low heart rate
        if all(value < 50 for value in patient3) == True:
            logger.info(f'''
Patient 3 Bradycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
      
    ch.basic_ack(delivery_tag=method.delivery_tag)


def p4_callback(ch, method, properties, body):
    """ 
    Patient 4 queue callback function
    """
    # unpack struct of hr_producer
    timestamp, heartrate = struct.unpack('!df', body) 

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S.%f")
    logger.info(f' [Patient 4 Current Heart Rate]: {heartrate} bpm : {timestamp_str}')

    # Add new heartrate
    patient4.append(heartrate)

    # check for heart rate once deque is at maxlen
    if len(patient4) == patient4.maxlen:
        # check for too high heart rate and alert if all values in deque are over threshold
        if all(value > 100 for value in patient4) == True:
            logger.info(f'''
Patient 4 Tachycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
        #check for too low heart rate
        if all(value < 50 for value in patient4) == True:
            logger.info(f'''
Patient 4 Bradycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
      
    ch.basic_ack(delivery_tag=method.delivery_tag)

def p5_callback(ch, method, properties, body):
    """ 
    Patient 5 queue callback function
    """
    # unpack struct of hr_producer
    timestamp, heartrate = struct.unpack('!df', body) 

    # convert timestamp back to string
    timestamp_str = datetime.fromtimestamp(timestamp).strftime("%m/%d/%y %H:%M:%S.%f")
    logger.info(f' [Patient 5 Current Heart Rate]: {heartrate} bpm : {timestamp_str}')

    # Add new heartrate
    patient5.append(heartrate)

    # check for heart rate once deque is at maxlen
    if len(patient5) == patient5.maxlen:
        # check for too high heart rate and alert if all values in deque are over threshold
        if all(value > 100 for value in patient5) == True:
            logger.info(f'''
Patient 5 Tachycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
        #check for too low heart rate
        if all(value < 50 for value in patient5) == True:
            logger.info(f'''
Patient 5 Bradycardia Alert!
Time of Alert: {timestamp_str}
Heart Rate at Time of Alert: {heartrate}''')
      
    ch.basic_ack(delivery_tag=method.delivery_tag)


# define a main Function
def main(hn: str = "localhost"):
    """ Continuously listen for messages"""

    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()

        # Declare queues
        queues = ('Patient_1', 'Patient_2', 'Patient_3', 'Patient_4', 'Patient_5')

        # create durable queue for each queue
        for queue in queues:
            # delete queue if it exists
            channel.queue_delete(queue=queue)
            # create new durable queue
            channel.queue_declare(queue=queue, durable=True)

        # restrict worker to one unread message at a time
        channel.basic_qos(prefetch_count=1) 

        # listen to each queue and execute corresponding callback function
        channel.basic_consume( queue='Patient_1', on_message_callback=p1_callback)
        channel.basic_consume( queue='Patient_2', on_message_callback=p2_callback)
        channel.basic_consume( queue='Patient_3', on_message_callback=p3_callback)
        channel.basic_consume( queue='Patient_4', on_message_callback=p4_callback)
        channel.basic_consume( queue='Patient_5', on_message_callback=p5_callback)
       
        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":
    # call the main function with the information needed
    main("localhost")