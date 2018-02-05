<?php

// source
// https://stackoverflow.com/questions/17902483/show-values-from-a-mysql-database-table-inside-a-html-table-on-a-webpage

$host    = "127.0.0.1";
$user    = "root";
$pass    = "cloudera";
$db_name = "insider_movies";
$query = "SELECT title, vote_average, genres, budget, revenue, date FROM movie  
	  WHERE vote_count >= 10
	  ORDER BY vote_average DESC, date DESC
	  LIMIT 50";

$myfile = fopen("insider_movies.html", "w") or die("Unable to open file!");
$html = "<html>".PHP_EOL;
$html .= "<head><style>".PHP_EOL;
$html .= "table { font-family: arial, sans-serif; border-collapse: collapse; width: 100%; }".PHP_EOL;
$html .= "td, th { border: 1px solid #dddddd; text-align: left; padding: 8px; }".PHP_EOL;
$html .= "tr:nth-child(even) { background-color: #dddddd; }".PHP_EOL;
$html .= "</style></head>".PHP_EOL;
$html .= "<body>".PHP_EOL;

//create connection
$connection = mysqli_connect($host, $user, $pass, $db_name);

//test if connection failed
if(mysqli_connect_errno()){
    die("connection failed: "
        . mysqli_connect_error()
        . " (" . mysqli_connect_errno()
        . ")");
}

//get results from database
$result = mysqli_query($connection, $query) or die("No movies found!");
$all_property = array();  //declare an array for saving property

//showing property
$html .= '<table class="data-table">'.PHP_EOL;
$html .= '<tr class="data-heading">'.PHP_EOL;  //initialize table tag
while ($property = mysqli_fetch_field($result)) {
    $html .= '<td>' . $property->name . '</td>'.PHP_EOL;  //get field name for header
    array_push($all_property, $property->name);  //save those to array
}
$html .= '</tr>'.PHP_EOL; //end tr tag

//showing all data
while ($row = mysqli_fetch_array($result)) {
    $html .= "<tr>".PHP_EOL;
    foreach ($all_property as $item) {
        $html .= '<td>' . $row[$item] . '</td>'.PHP_EOL; //get items using property value
    }
    $html .= '</tr>'.PHP_EOL;
}
$html .= "</table>".PHP_EOL;

$html .= "</html></body>";
fwrite($myfile, $html);
fclose($myfile);
?>
