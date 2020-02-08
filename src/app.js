const SchemaValidator = require("./schema_validator");
const JSONFileWriter = require('./json_writer');
const Aggregator = require("./aggregator");
const log = require('./logging');

const fs = require('fs');
const csv = require('fast-csv');
const https = require('https');
var HashMap = require('hashmap');

log.info('Running ...');
const csvParser = csv({headers: true, trim: true});

var fareDetails;
var map = new HashMap();
getData("https://s3.eu-north-1.amazonaws.com/lemon-1/rate.json", function (status, data) {
    if(status === "Success") {
        data.forEach(function (row) {
            map.set(row.zone, row.price);
        });

        let url = "https://s3.eu-north-1.amazonaws.com/lemon-1/scooter_1337.csv";
        const file = fs.createWriteStream("temp.csv");
        https.get(url, response => {
            var stream = response.pipe(file);
            stream.on("finish", function() {
                const fileReadStream = fs.createReadStream('./temp.csv', 'utf8');
                fileReadStream.pipe(csvParser)
                    .pipe(new SchemaValidator())
                    .pipe(new Aggregator({fareDetailsMap: map}))
                    .pipe(new JSONFileWriter({prefix: 'output_'}));
            });
        });
    }
});

function getData(url, callback) {
    https.get(url, (res) => {
        let body = "";
        res.on("data", (chunk) => {
            body += chunk;
        });
        res.on("end", () => {
            try {
                this.fareDetails = JSON.parse(body);
                callback("Success", JSON.parse(body));
            } catch (e) {
                console.error("Error while loading the fare details from json file.")
            };
        });
    }).on("error", (error) => {
        console.error(error.message);
    });
}