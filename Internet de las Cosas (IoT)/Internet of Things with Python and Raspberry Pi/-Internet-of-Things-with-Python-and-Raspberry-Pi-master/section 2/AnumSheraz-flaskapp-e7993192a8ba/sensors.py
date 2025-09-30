import RPi.GPIO as GPIO
import time

PIR_pin = 23
Buzzer_pin = 24

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)
GPIO.setup(PIR_pin, GPIO.IN)
GPIO.setup(Buzzer_pin, GPIO.OUT)

def beep(repeat):
   for i in range(0, repeat):
      for pulse in range(60):  # square wave loop
         GPIO.output(7, True)
         time.sleep(0.001)     # high for 1 millisec
         GPIO.output(7, False)      
         time.sleep(0.001)     # low for 1 millisec
      time.sleep(0.02)         # add a pause between each cycle
	  
def motionDetection():
    while True:
	  if GPIO.input(PIR_pin):
	     print("Motion detected!")
		 beep(4)
	  time.sleep(1)
	  
time.sleep(1)
motionDetection()