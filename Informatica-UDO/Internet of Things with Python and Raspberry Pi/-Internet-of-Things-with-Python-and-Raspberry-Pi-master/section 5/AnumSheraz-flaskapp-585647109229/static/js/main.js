    var myChannel = "RSPY";

    $(function(argument) {
      $('[type="checkbox"]').bootstrapSwitch();
	  $.fn.bootstrapSwitch.defaults.labelWidth = 400;
    });

	$('input').on('switchChange.bootstrapSwitch', function (event, state) {
        console.log("EVENT>>>" , this.id, state);
        var btnStatus = new Object();
        //btnStatus[this.id] = $(this).data(state ? 'onText' : 'offText');
        btnStatus[this.id] = state;
        console.log(btnStatus);

        var event = new Object();
        event.event = btnStatus;
        //sendEvent(this.id + "-" + value);
        publishUpdate(event, myChannel);
    })

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
      subscribe_key: 'sub-c-c96cd480-3528-11e8-a218-f214888d2de6'

    });


    pubnub.addListener({
        status: function(statusEvent) {
            if (statusEvent.category === "PNConnectedCategory") {
                //publishSampleMessage();
            }
        },
        message: function(message) {
        var msg = message.message;
        if (msg.event){
            $("#motion_id").text(msg.event["motion"]);

         }
        },

        presence: function(presenceEvent) {
            // handle presence
        }
    })

    pubnub.subscribe({
        channels: [myChannel]
    });


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