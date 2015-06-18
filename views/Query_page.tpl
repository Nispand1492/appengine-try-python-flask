<html>
<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.2/jquery.min.js">
$(document).ready(function()
{
    $(#butt).click(function()
    {
       var postObj = {};
                        postObj["magnitude"] = $( "#magnitude" ).val();
                        postObj["par1"] = $( "#par1" ).val();
                        postObj["loc"] = $( "#loc" ).val();
                        postObj["par2"] = $( "#par2" ).val();
                        console.log(postObj);
                        if ( between(parseInt(postObj["magnitude"]),1,15) ){
               			$.post("/process_query",postObj,
    				function(data, status){
        				//alert("Data: " + data + "\nStatus: " + status);
                                	//$("#content").text(data);
                                        addContent(data);
               			});
                       }
                       else {
                              $("#content").text("Invalid magnitude rage<br>");
                       }
    })
    function between(x, min, max) {
        return x >= min && x <= max;
    }
    function addContent(resp){
      var resp = JSON.parse(resp);
      html_string = "<table><th></th>";
      for (var x in resp) {
      	html_string += "<tr>";
      	for (var y in resp[x] ){
      		html_string +="<td>"+resp[x][y]+"</td>";
      	}
      	html_string += "</tr>";
      }
      html_string += "</table>";
      console.log(html_string);
      $("#content").html(html_string);

})
</script>

<body>
    <form method ="POST" enctype = "multipart/form-data">
        <h1>Enter Your Data::</h1>
        Magnitude::<input type = "text" id = "magnitude" name = 'mag'> <br>
        Parameter1: <select id = "par1" name = 'par1'>
        <option value = "grt">Greater than </option>
        <option value = "less">Less than </option>
        <option value = "equal">Equal </option>
        <option value = "lte"> Less than equal to </option>
        <option value = "gte"> Greater than equal to </option>
        <input type = "submit" value = "Submit" id ="butt"></br>
        Parameter2::<select id = 'par2'>
        <option value = "and">AND</option>
        <option value = "or">OR</option>
        </select>
        Location:: <input type = "text" id = "loc">
        <input type = "button" id = "butt" value = "getdata">
    </form>
    <div id = "content">
    </div>
  <body>
</html>