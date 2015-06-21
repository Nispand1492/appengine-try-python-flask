<html>



<body>
    <form action = "/process_query" method ="GET" enctype = "multipart/form-data">
        <h1>Enter Your Data::</h1>
        Magnitude::<input type = "text" id = "magnitude" name = 'mag'> <br>
        Parameter1: <select id = "par1" name = 'par1'>
        <option value = "grt">Greater than </option>
        <option value = "less">Less than </option>
        <option value = "equal">Equal </option>
        <option value = "lte"> Less than equal to </option>
        <option value = "gte"> Greater than equal to </option>
        <input type = "submit" value = "Submit" id ="butt"></br></br>
        Parameter2::<select id = 'par2'>
        <option value = "and">AND</option>
        <option value = "or">OR</option>
        </select>
        Location:: <input type = "text" id = "loc">
        <input type = "submit" id = "butt" value = "getdata">
    </form>
    <div id = "content">
    </div>
  <body>
</html>