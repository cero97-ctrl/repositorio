import RPi.GPIO as GPIO
import time, threading
from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub import PubNub

pnconfig = PNConfiguration()

pnconfig.subscribe_key = 'sub-c-c96cd480-3528-11e8-a218-f214888d2de6'
pnconfig.publish_key = 'pub-c-f141a42f-ae6d-4f11-bbaf-4bc7cb518b6c'
pubnub = PubNub(pnconfig)

myChannel = "RSPY"
PIR_pin = 23
Buzzer_pin = 24
sensorsList = ["buzzer"]
data = {}

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_pin, GPIO.IN)
GPIO.setup(Buzzer_pin, GPIO.OUT)



def beep(repeat):
   for i in range(0, repeat):
      for pulse in range(60):  # square wave loop
         GPIO.output(Buzzer_pin, True)
         time.sleep(0.001)     # high for 1 millisec
         GPIO.output(Buzzer_pin, False)      
         time.sleep(0.001)     # low for 1 millisec
      time.sleep(0.02)         # add a pause between each cycle   
                  
def motionDetection():
    data["alarm"] = False
    print("sensors started")
    trigger = False    
    while True:
       if GPIO.input(PIR_pin):
          print("Motion detected!")
          beep(4)
          trigger = True
          publish(myChannel, {"motion": "Yes"})
          time.sleep(1)
       elif trigger:
		  publish(myChannel, {"motion": "No"})
		  trigger = False   
          
       if data["alarm"]:
		  beep(2)
          
def publish(channel, msg):
	pubnub.publish().channel(channel).message(msg).async(my_publish_callback)

def my_publish_callback(envelope, status):
    # Check whether request successfully completed or not
    if not status.is_error():
        pass  # Message successfully published to specified channel.
    else:
        pass  # Handle message publish error. Check 'category' property to find out possible issue
        # because of which request did fail.
        # Request can be resent using: [status retry];


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
            #send("")
            pubnub.publish().channel(myChannel).message("Device connected!!").async(my_publish_callback)
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
        try:
            print(message.message, ": ", type(message.message))
            msg = message.message
            print("received json:", msg)
            key = list(msg.keys())
            if (key[0]) == "event":    # {"event": {"sensor_name": True } }
                self.handleEvent(msg)
        except Exception  as e:
            print("received:", message.message)
            print(e)
            pass

    def handleEvent(self, msg):
        global data
        eventData = msg["event"]
        key = list(eventData.keys())
        if key[0] in sensorsList:  
           if eventData[key[0]] is True:
			  data["alarm"] = True
           elif	eventData[key[0]] is False:
			  data["alarm"] = False      

if __name__ == '__main__':
	sensorsThread = threading.Thread(target=motionDetection)
	sensorsThread.start()

	pubnub.add_listener(MySubscribeCallback())
	pubnub.subscribe().channels(myChannel).execute()
