    var myChannel = "RSPY";

    //Ask Server to give PubNub authorisation key
    sendEvent("get_authKey");

    $(function(argument) {
      $('[type="checkbox"]').bootstrapSwitch();
	  $.fn.bootstrapSwitch.defaults.labelWidth = 400;
    });

	$('input').on('switchChange.bootstrapSwitch', function (event, state) {
        //ID of all switch buttons that sends msg over PubNub channel starts with 'event'
        if (this.id.startsWith("event")){
            console.log("EVENT>>>" , this.id, state);
            var btnStatus = new Object();
            btnStatus[this.id.split("-")[1]] = state;
            var event = new Object();
            event.event = btnStatus;
            publishUpdate(event, myChannel);
        }
    });

  $("button").click(function() {
       if (this.id.startsWith("access-user")){
           var userId = this.id.split("-")[2];
           var read_state  = $('#read-user-'  + userId).bootstrapSwitch('state');
           var write_state = $('#write-user-' + userId).bootstrapSwitch('state');
           sendEvent("grant-user-" + userId + "-" + read_state + "-" + write_state);
       } else if (this.id.startsWith("access-device")){
           var deviceAuthKey = $('#device-auth-key').val();
           var read_state  = $('#read-device').bootstrapSwitch('state');
           var write_state = $('#write-device').bootstrapSwitch('state');
           sendEvent("grant-device-" + deviceAuthKey + "-" + read_state + "-" + write_state);
       }
  });

   function sendEvent(value) {
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(){
      if(this.readyState === 4){
            if (this.status === 200) {
              if (this.responseText !== null) {
                try{
                  var json_data = this.responseText;
		          var json_obj = JSON.parse(json_data);
                  if(json_obj.hasOwnProperty('authKey')){
                     pubnub.setAuthKey(json_obj.authKey);
                     pubnub.setCipherKey(json_obj.cipherKey);
                     console.log("Auth key set! " + this.responseText);
                     subscribe();
                  }
                  if(json_obj.hasOwnProperty('access')){
                     if (json_obj.access == "granted"){
                         console.log("access-granted!")
                         subscribe();
                     }
                  }
                }catch (e){
                  console.log("can't extract json: " + this.responseText);
                }
               }
            }
      }
    };
    console.log("sending ajax " + value)
    request.open("POST", value, true);
    request.send(null);
  }

  var alive_second = 0;
  var heartbeat_rate = 5000;

  function keep_alive(){
    var request = new XMLHttpRequest();
    request.onreadystatechange = function(){
      if(this.readyState === 4){
            if (this.status === 200) {
              if (this.responseText !== null) {
                var date = new Date();
                alive_second = date.getTime();
                var keep_alive_data=this.responseText;
                console.log(keep_alive_data)
                }
            }
        }
    };
    request.open("GET","keep_alive", true);
    request.send(null);
    setTimeout('keep_alive()', heartbeat_rate);
   }

  function time(){
            var d = new Date();
            var current_sec = d.getTime();
            if (current_sec - alive_second > heartbeat_rate + 1000){
               document.getElementById("Connection_id").innerHTML = " Dead";
            } else {
               document.getElementById("Connection_id").innerHTML = " Alive";
            }
    setTimeout('time()',1000);
   }

    pubnub = new PubNub({
      publish_key: 'pub-c-f141a42f-ae6d-4f11-bbaf-4bc7cb518b6c',
      subscribe_key: 'sub-c-c96cd480-3528-11e8-a218-f214888d2de6',
      ssl: true
    });


    pubnub.addListener({
        status: function(statusEvent) {
            if (statusEvent.category === "PNConnectedCategory") {
                //publishSampleMessage();
            }
        },
        message: function(message) {
        var msg = message.message;
            if (msg.motion){
                $("#motion_id").text(msg.motion);
            }
        }
    })

	  function subscribe(){
		pubnub.subscribe({
			channels: [myChannel],
		},
		function (status, response) {
			if (status.error) {
				console.log( "Subscribed Failed ", status)
			} else {
				console.log("Subscribed Success ", status)
			}
		 });
	  }


	function publishUpdate(data, channel) {
	  pubnub.publish({
		channel: channel,
		message: data
	  },
	      function (status, response) {
		if (status.error) {
		    console.log(status)
		} else {
		    console.log("message Published w/ timetoken", response.timetoken)
		}
	       }
	  );

	}

	function logout(){
	   	console.log('Logging out and unsubscribing');
		pubnub.unsubscribe({
			channels: [myChannel]
		})
	    location.replace("/logout");
        }

    var __eon_cols = ["tempC","tempF","hum"];
    var __eon_labels = {};
    chart = eon.chart({
      pubnub: pubnub,
      channels: [myChannel],
      history: false,
      flow: true,
      rate: 1000,
      limit: 50,
      generate: {
        bindto: "#atmosphere_chart",
        data: {
          colors: {"tempC":"#D70060","tempF":"#E54028","hum":"#F18D05"},
          type: "spline"
        },
        transition: {
          duration: 250
        },
        axis: {
          x: {
            label: ""
          },
          y: {
            label: ""
          }
        },
        grid: {
          x: {
            show: false
          },
          y: {
            show: false
          }
        },
        tooltip: {
         show: true
        },
        point: {
          show: false
        }
      },
      transform: function(message) {
        var msg = JSON.stringify(message);
        var json_data = JSON.parse(msg);
        if(json_data.hasOwnProperty('atmos')){
            var message = eon.c.flatten(message.atmos);
            var o = {};
            for(index in message) {
              if(__eon_cols.indexOf(index) > -1){
                o[__eon_labels[index] || index] = message[index];
              }
             }
            return {
              eon: o
            };
       }
      }
    });


	eon.chart({
	  pubnub: pubnub,
	  channels: [myChannel],
	  generate: {
		bindto: '#light_chart',
		data: {
		  type: 'gauge',
		},
		gauge: {
		  min: 0,
		  max: 100
		},
		color: {
		  pattern: ['#2eff00', '#ffee00', '#ff1900'],
		  threshold: {
			values: [30, 50, 70]
		  }
		}
	  },
	  transform: function(m) {
		var msg = JSON.stringify(m);
        var json_data = JSON.parse(msg);
        if(json_data.hasOwnProperty('light')){
			return { eon: {
			  light: json_data.light
			}}
		}
	  }
	});
