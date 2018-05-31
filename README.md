# Chicago Vacant Buildings Data Importer

This script pulls 311 reports of vacant and abandoned buildings from the City of Chicago Data Portal and dumps them to a CSV file.

Source: https://data.cityofchicago.org/Service-Requests/311-Service-Requests-Vacant-and-Abandoned-Building/7nii-7srd
Destination: http://www.google.com/fusiontables/DataSource?dsrcid=1614852 

This import script has 2 modes: 

- importing into Google Fusion Tables **deprecated**
- exporting to a CSV file

To use CSV mode, set `$dump_to_csv = true;` in `source/connectioninfo.php`

In CSV mode, the script pulls down a CSV from the Socrata data portal and removes any duplicate rows for addresses that it has already seen.

usage
1. `cp source/connectioninfo.php.example to source/connectioninfo.php`
2. save your settings in source/connectioninfo.php
3. `php run_import.php`
