-------------
 Assignment:
-------------

LemonScooters provides a electric scooter rental service.
This stream fetch a file containing log records of all rental events, it validates and removes any invalid record, 
aggregates the total rental time for each customer and outputs a json file with the aggregated records.

The format of the csv file is:

customerId - The id of the customer
startTime - The the scooter rental started
endTime - The time the scoother was returned
zone - In which zone of the town the scooter was returned

Now change the stream so that it will:

1) Collect the csv file using http from the following URL: https://s3.eu-north-1.amazonaws.com/lemon-1/scooter_1337.csv

2) Write all invalid records to a json file with the follwing pattern: error_<timestamp>.json
   Records are invalid when they do not match the JSON schema.

3) The stream should also calculates how much each customer should pay.
   The price should depend on where the scooter is parked, e.g parking the scooter in the city center is cheeper then in the surrounding areas.
   The actual rate should be fetched by the stream from the following URL: https://s3.eu-north-1.amazonaws.com/lemon-1/rate.json


Instructions to run the application

1. Go to assigment directory.
2. npm install
3. run the app.js.
4. Once the application completes, we can see 2 files generated in working directory.
    1. File for invalid records.
    2. File for output with fare details.
Note: I have used extra node modules, you may have to install them.