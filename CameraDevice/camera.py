from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import json
from datetime import datetime
import time

CAMERA_ID = 1234
GATE_ID = 4321

# Configs
iot_core_endpoint = 'a16uyg6g85q8ne-ats.iot.us-east-1.amazonaws.com'
root_ca_path = './certs/root.pem'
private_key_path = './certs/private.pem.key'
certificate_path = './certs/certificate.pem.crt'
topic = 'iot/tollCollectionSystem/carAtGate'

my_mqtt_client = AWSIoTMQTTClient('camera-1234')


def open_connection():
    my_mqtt_client.configureEndpoint(iot_core_endpoint, 8883)
    my_mqtt_client.configureCredentials(root_ca_path, private_key_path, certificate_path)
    my_mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
    my_mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
    my_mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
    my_mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec
    my_mqtt_client.connect()
    print('{} - The connection has been opened'.format(datetime.now()))


def send_message(payload):
    print('{} - New message published'.format(datetime.now()))
    my_mqtt_client.publish(topic, payload, 0)


open_connection()
with open('cars.json', encoding="utf8") as json_file:
    cars = json.load(json_file)
    for car in cars['data']:
        if 'tablica-rejestracyjna' in car['attributes']:
            data = {
                "cameraId": CAMERA_ID,
                "gateId": GATE_ID,
                "date": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f%z'),
                "car": {
                    "plateNumber": car['attributes']['tablica-rejestracyjna'],
                    "make": car['attributes']['marka'],
                    "model": car['attributes']['model']
                }
            }
            print('{} - Car at gate: {}'.format(datetime.now(), data))
            send_message(str(data))
            time.sleep(5)
