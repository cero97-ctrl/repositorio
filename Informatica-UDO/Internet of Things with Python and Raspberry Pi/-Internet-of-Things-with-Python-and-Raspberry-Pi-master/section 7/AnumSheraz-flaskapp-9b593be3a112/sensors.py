import RPi.GPIO as GPIO
import Adafruit_DHT
import time, threading, spidev
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

pnconfig = PNConfiguration()

pnconfig.subscribe_key = 'sub-c-c96cd480-3528-11e8-a218-f214888d2de6'
pnconfig.publish_key = 'pub-c-f141a42f-ae6d-4f11-bbaf-4bc7cb518b6c'
##########################
pnconfig.cipher_key = 'myCipherKey'
pnconfig.auth_key = 'raspberry-pi'
pubnub = PubNub(pnconfig)

myChannel = "RSPY"
PIR_pin = 23
Buzzer_pin = 24
dht11_pin = 5
sensorsList = ["buzzer"]
data = {}
# Define MCP3008 channels
light_channel = 0

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_pin, GPIO.IN)
GPIO.setup(Buzzer_pin, GPIO.OUT)

# Open SPI bus
spi = spidev.SpiDev()
spi.open(0,0)
spi.max_speed_hz=1000000

# https://stackoverflow.com/a/23157705/5167801
def scale(valueIn, baseMin, baseMax, limitMin, limitMax):
        return ((limitMax - limitMin) * (valueIn - baseMin) / (baseMax - baseMin)) + limitMin


# Function to read SPI data from MCP3008 chip
# Channel must be an integer 0-7
def ReadChannel(channel):
  adc = spi.xfer2([1,(8+channel)<<4,0])
  data = ((adc[1]&3) << 8) + adc[2]
  return scale(data, 10, 700, 0, 100)


def my_publish_callback(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass  # Message successfully published to specified channel.
    else:
		print("Unable to send message:", e)


def publish(channel, msg):
    pubnub.publish().channel(channel).message(msg).async(my_publish_callback)


def beep(repeat):
    for i in range(0, repeat):
        for pulse in range(60):  # square wave loop
            GPIO.output(Buzzer_pin, True)
            time.sleep(0.001)  # high for 1 millisec
            GPIO.output(Buzzer_pin, False)
            time.sleep(0.001)  # low for 1 millisec
        time.sleep(0.02)  # add a pause between each cycle


def motionDetection():
    data["alarm"] = False
    print("sensors started")
    trigger = False
    while True:
        publish(myChannel, {"light": str(ReadChannel(light_channel))})
        time.sleep(1) # give some rest to Raspberry Pi
        if GPIO.input(PIR_pin):
            beep(4)
            trigger = True
            publish(myChannel, {"motion": "Yes"})
            print('motion detected!')
            time.sleep(0.5)
        elif trigger:
            publish(myChannel, {"motion": "No"})
            trigger = False

        if data["alarm"]:
            beep(2)

def readDht11():
	while True:
		hum, tempC = Adafruit_DHT.read_retry(11, dht11_pin)
		tempF = tempC * 9/5.0 + 32
		publish(myChannel, {"atmos": {"tempC": str(tempC), "tempF": str(tempF), "hum": hum}})

class MySubscribeCallback(SubscribeCallback):
    def presence(self, pubnub, presence):
        pass  # handle incoming presence data

    def status(self, pubnub, status):
        if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
            pass  # This event happens when radio / connectivity is lost

        elif status.category == PNStatusCategory.PNConnectedCategory:
            # Connect event. You can do stuff like publish, and know you'll get it.
            # Or just use the connected event to confirm you are subscribed for
            # UI / internal notifications, etc
            # send("")
            publish(myChannel, "Device connected!!")
        elif status.category == PNStatusCategory.PNReconnectedCategory:
            pass
            # Happens as part of our regular operation. This event happens when
            # radio / connectivity is lost, then regained.
        elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
            pass
            # Handle message decryption error. Probably client configured to
            # encrypt messages and on live data feed it received plain text.

    def message(self, pubnub, message):
        global data
        print(message.message)
        try:
            msg = message.message
            key = list(msg.keys())
            if (key[0]) == "event":  # {"event": {"sensor_name": True } }
                self.handleEvent(msg)
        except Exception  as e:
            print("Receiving message: ", message.message)

    def handleEvent(self, msg):
        global data
        eventData = msg["event"]
        key = list(eventData.keys())
        if key[0] in sensorsList:
            if eventData[key[0]] is True:
                data["alarm"] = True
            elif eventData[key[0]] is False:
                data["alarm"] = False


if __name__ == '__main__':
    pubnub.add_listener(MySubscribeCallback())
    pubnub.subscribe().channels(myChannel).execute()

    time.sleep(3)
    sensorsThread = threading.Thread(target=motionDetection)
    sensorsThread.start()

    dhtThread = threading.Thread(target=readDht11)
    dhtThread.start()
