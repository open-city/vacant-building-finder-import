# Chicago Vacant Buildings Data Importer

This script pulls 311 reports of vacant and abandoned buildings from the City of Chicago Data Portal and dumps them to a CSV file.

Source: https://data.cityofchicago.org/Service-Requests/311-Service-Requests-Vacant-and-Abandoned-Building/7nii-7srd
Destination: http://www.google.com/fusiontables/DataSource?dsrcid=1614852 

usage
1. copy source/connectioninfo.php.example to source/connectioninfo.php
2. fill in your Socrata and Google account info in source/connectioninfo.php
2. $ php run_import.php