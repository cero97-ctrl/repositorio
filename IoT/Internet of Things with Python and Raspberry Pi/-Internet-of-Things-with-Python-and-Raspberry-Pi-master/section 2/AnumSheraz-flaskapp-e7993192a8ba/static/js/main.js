    $(function(argument) {
      $('[type="checkbox"]').bootstrapSwitch();
	  $.fn.bootstrapSwitch.defaults.labelWidth = 400;
    });

	$('input').on('switchChange.bootstrapSwitch', function (event, state) {
        console.log("EVENT>>>" , this.id, state);
        var d_name = this.id;
        var value = $(this).data(state ? 'onText' : 'offText');
        console.log(this.id + ": " + value);
        sendEvent(this.id + "-" + value);
    })
    
       function sendEvent(value) {
	    var request = new XMLHttpRequest();
	    request.onreadystatechange = function(){
	      if(this.readyState === 4){
                if (this.status === 200) {
                  if (this.responseText !== null) {
					//document.getElementById("btn_stats").innerHTML = this.responseText;
				   }
                }
	      }
	    };
	    request.open("POST", "status=" + value, true);
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
                //convert the string to JSON
                var json_data=this.responseText;
				var json_obj=JSON.parse(json_data);
				if(json_obj.motion == 1){
				     document.getElementById("motion_id").innerHTML = " Yes";
				} else {
				     document.getElementById("motion_id").innerHTML = " No";
				}	                
                           
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
