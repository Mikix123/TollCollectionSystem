from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from datetime import datetime
import time

GATE_ID = 4321

# Configs
iot_core_endpoint = 'a16uyg6g85q8ne-ats.iot.us-east-1.amazonaws.com'
root_ca_path = './certs/root.pem'
private_key_path = './certs/private.pem.key'
certificate_path = './certs/certificate.pem.crt'
topic = 'iot/tollCollectionSystem/openTheGate{}'.format(GATE_ID)

my_mqtt_client = AWSIoTMQTTClient('gate-4321')


def callback(client, user_data, message):
    print("{} - Open the gate.".format(datetime.now()))


def open_connection():
    my_mqtt_client.configureEndpoint(iot_core_endpoint, 8883)
    my_mqtt_client.configureCredentials(root_ca_path, private_key_path, certificate_path)
    my_mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
    my_mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
    my_mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
    my_mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec
    my_mqtt_client.connect()
    print('{} - The connection has been opened'.format(datetime.now()))


open_connection()
my_mqtt_client.subscribe(topic, 0, callback)

while True:
    time.sleep(0.5)
