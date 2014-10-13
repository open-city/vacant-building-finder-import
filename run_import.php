<?php
  //see README for instructions
  error_reporting(E_ALL);
  ini_set("memory_limit","800M"); //datasets we are dealing with can be quite large, need enough space in memory
  set_time_limit(0);
  date_default_timezone_set('America/Chicago');
  
  //inserting in to Fusion Tables with http://code.google.com/p/fusion-tables-client-php/
  require('source/clientlogin.php');
  require('source/sql.php');
  require('source/file.php'); //not being used, but could be useful to someone automating CSV import in to FT
  require('source/connectioninfo.php');

  //keep track of script execution time
  $bgtime=time();

  //if this flag is set to true, no Fusion Table inserts will be made and everything will be saved to a CSV
  $dump_to_csv = ConnectionInfo::$dump_to_csv;
  $url  = ConnectionInfo::$url;
  $path = ConnectionInfo::$path;
  
  echo "Chicago Vacant Building Finder import by Derek Eder\n";
  echo "Downloading from $url... \n";

  $ch = curl_init($url);
  curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
  $data = curl_exec($ch);
  curl_close($ch);
  file_put_contents($path, $data);

  $saved_file = fopen($path, 'r');
  $buildings_array = array();
  while (($line = fgetcsv($saved_file)) !== FALSE) {
    //$line is an array of the csv elements
    $buildings_array[] = $line;
  }
  fclose($saved_file);
  
  if(count($buildings_array) > 0) {

    if ($dump_to_csv) {
        echo "\nDumping to vacant_buildings.csv ...\n";
        //Saving all results to CSV and doing a full replace in Fusion Tables
        $fp = fopen('vacant_buildings.csv', 'w+');
    }
    else {
      echo "\nInserting in to Fusion Tables ...\n";
      echo "\nChecking for reported buildings in the last " . ConnectionInfo::$date_start_interval . "\n";
      //Fetch info from Fusion Tables and do inserts & data manipulation
      
      //get token
      $fusionTableId = ConnectionInfo::$fusionTableId;
    	$token = ClientLogin::getAuthToken(ConnectionInfo::$google_username, ConnectionInfo::$google_password);
    	$ftclient = new FTClientLogin($token);
    	
    	//for clearing out table
      //echo "\nClearing out existing data\n";
    	//$ftclient->query("DELETE FROM $fusionTableId");
    }

  	$insertCount = 0;
    $updateCount = 0;
    $processCount = 0;
  	$unique_addresses = array();

    $date_start = new DateTime('@' . strtotime('-1 ' . ConnectionInfo::$date_start_interval));

    foreach($buildings_array as $row) {
      if ($row[1] != "SERVICE REQUEST NUMBER") { //skip the header row
        $processCount++;
        if ($processCount % 1000 == 0) 
          echo "processed $processCount rows\n";
        //echo var_dump($row);
    		//convert received date in to DateTime format
    		$receivedDate = new DateTime($row[2]);
    		
    		//creating full address column for geocoding
    		$fullAddress = $row[10] . " " . $row[11] . " " . $row[12] . " " . $row[13] . " chicago IL " . $row[14];
    		
      	$insertArray = array(
      	"SERVICE REQUEST #" => $row[1],
      	"DATE RECEIVED" => $receivedDate->format('m/d/Y'),
      	"LOT LOCATION" => $row[3],
      	"DANGEROUS OR HAZARDOUS?" => $row[4], //this column appears to be empty
      	"Dangerous flag" => SQLBuilder::convertToFlag($row[4], "dangerous"),
      	"OPEN OR BOARDED?" => $row[5],
      	"Open flag" => SQLBuilder::convertToFlag($row[5], "open"),
      	"ENTRY POINT" => SQLBuilder::escape_string($row[6]),
      	"VACANT OR OCCUPIED?" => $row[7],
      	"Vacant flag" => SQLBuilder::convertToFlag($row[7], "vacant"),
      	"VACANT DUE TO FIRE?" => SQLBuilder::setEmptyToZero($row[8]),
      	"Fire flag" => SQLBuilder::setEmptyToZero($row[8]), //stored as an int in Socrata
      	"ANY PEOPLE USING PROPERTY?" => SQLBuilder::setEmptyToZero($row[9]),
      	"In use flag" => SQLBuilder::setEmptyToZero($row[9]), //stored as an int in Socrata
      	"ADDRESS STREET NUMBER" => $row[10],
      	"ADDRESS STREET DIRECTION" => $row[11],
      	"ADDRESS STREET NAME" => $row[12],
      	"ADDRESS STREET SUFFIX" => $row[13],
      	"ZIP CODE" => $row[14],
      	"Full Address" => $fullAddress,
      	"X COORDINATE" => $row[15],
      	"Y COORDINATE" => $row[16],
      	"Ward" => $row[17],
      	"Police District" => $row[18],
      	"Community Area" => $row[19],
      	"LATITUDE" => $row[20],
      	"LONGITUDE" => $row[21],
      	"Location" => "$row[20],$row[21]"
      	);

        if (!in_array($fullAddress, $unique_addresses)) {
          if ($dump_to_csv) {
            fputcsv($fp, $insertArray); //save to CSV
            //keep track of addresses inserted
            array_push($unique_addresses, $fullAddress);
            $insertCount++;
          }
          else if ($receivedDate > $date_start){
            //only look at rows in the last month
            $row_received_date = fetch_by_address("'DATE RECEIVED'", $fullAddress, $ftclient, $fusionTableId);
            if ($row_received_date == NULL) {
              $ftclient->query(SQLBuilder::insert($fusionTableId, $insertArray));

              //FT has an insert throughput limit of 0.5 qps defined here: 
              //https://developers.google.com/fusiontables/docs/v1/using#Geo
              sleep(1); 
              $insertCount++;
              echo "inserted $insertCount so far: " . $fullAddress . "\n";
            }
            else if (new DateTime($row_received_date) < $receivedDate) {
              $updated_date = new DateTime($row_received_date);
              $row_id = fetch_by_address('ROWID', $fullAddress, $ftclient, $fusionTableId);
              $ftclient->query(SQLBuilder::update($fusionTableId, $insertArray, $row_id));
              $updateCount++;
              echo "updated $updateCount so far: " . $fullAddress . " | " . $receivedDate->format('m/d/Y') . " > " . $updated_date->format('m/d/Y') . "\n";
            }
          }
          else {
            break; //if we hit a row that is older than one month, exit the loop
          }
        }
      }
  	}
  }
  echo "\ninserted $insertCount rows\n";
  echo "updated $updateCount rows\n";
  echo "This script ran in " . (time()-$bgtime) . " seconds\n";
  echo "\nDone.\n";

  function fetch_by_address($column, $fullAddress, $ftclient, $fusionTableId) {
    $response = $ftclient->query(SQLBuilder::select($fusionTableId, "$column", "'Full Address' = '$fullAddress'"));

    //echo var_dump($response);
    if (count($response) > 1)
      return $response[1][0];
    else
      return NULL;
  }

?>
