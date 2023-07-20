import Adafruit_DHT
from datetime import datetime
import RPi.GPIO as GPIO
import psycopg2
import time
import paho.mqtt.client as mqtt
import json

MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883

def on_connect(client, userdata, flag, rc):
	print("Connected")
	
	
def on_publish(client, userdata, mid):
	print("published")
	
#mqtt client setup
client = mqtt.Client()
client.on_connect = on_connect
client.on_publish = on_publish

print("Connwection result: ", client.connect(MQTT_BROKER, MQTT_PORT,60))

client.loop_start()

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(17, GPIO.OUT)

dhtPin = 4

while True:
    
        sensor = Adafruit_DHT.DHT11
        humidity, temperature = Adafruit_DHT.read_retry(sensor, dhtPin)

        #get current date and time
        current_time = datetime.now().time().strftime('%H:%M:%S')
        current_date = datetime.now().date()


        print('Temp={0:0.1f}*C Humidty={1:0.1f}%'.format(temperature, humidity))
        print("Time : ", current_time)
        print("Date : ", current_date)
        
        
        if humidity >= 70:
            print('Relay On')
            GPIO.output(17, GPIO.HIGH)
            
        
        else:
            print('Relay Off')
            GPIO.output(17, GPIO.LOW)
            
    
        payload = {
                "temperature" : temperature,
                "humidity" : humidity,
                "date_time" : time.strftime("%Y-%m-%d %H:%M:%S") 
        }
        print(payload)
        
        client.publish("payload", json.dumps(payload))
        #print("")
        
        time.sleep(10)
           
        
