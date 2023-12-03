"use strict";
const http = require("http");
var assert = require("assert");
const express = require("express");
const app = express();
const mustache = require("mustache");
const filesystem = require("fs");
const url = require("url");
const port = Number(process.argv[2]);

const hbase = require("hbase");
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4]) });

const columnsToConvert = [
  "md:median_sale_price",
  "md:median_list_price",
  "md:homes_sold",
  "md:pending_sales",
  "md:new_listings",
  "md:inventory",
  "md:median_dom",
  "md:off_market_in_two_weeks",
];

function binaryToNumber(c) {
  return Number(Buffer.from(c, "latin1").readBigInt64BE());
}
function rowToMap(row) {
  var stats = {};
  row.forEach(function (item) {
    const column = item["column"];

    // Check if the column is in the list of columns to convert
    if (columnsToConvert.includes(column)) {
      // Call counterToNumber only for the specified columns
      stats[column] = binaryToNumber(item["$"]);
    } else {
      // For columns not in the list, directly assign the value
      stats[column] = item["$"];
    }
  });
  return stats;
}

hclient
  .table("yvesyang_zip_estate")
  .row("2022_10_60607_-1")
  .get((error, value) => {
    console.info(rowToMap(value));
    // console.info(value);
  });

app.get("/market_data.html", function (req, res) {
  const record_key =
    req.query["year"] +
    "_" +
    req.query["month"] +
    "_" +
    req.query["zipcode"] +
    "_" +
    req.query["type"];
  console.log(record_key);
  hclient
    .table("yvesyang_zip_estate")
    .row(record_key)
    .get(function (err, record) {
      const dataResult = rowToMap(record);
      console.log(dataResult);

      var template = filesystem.readFileSync("result.mustache").toString();
      var html = mustache.render(template, {
        year: req.query["year"],
        month: req.query["month"],
        zipcode: req.query["zipcode"],
        type: req.query["type"],
        city: dataResult["md:city"],
        state: dataResult["md:state"],
        latitude: dataResult["md:latitude"],
        longitude:dataResult["md:longitude"],
        city_state: dataResult["md:city_state"],
        property_type: dataResult["md:property_type"],
        property_type_id: dataResult["md:property_type_id"],
        median_sale_price: dataResult["md:median_sale_price"],
        median_list_price: dataResult["md:median_list_price"],
        home_sold: dataResult["md:homes_sold"],
        pending_sales: dataResult["md:pending_sales"],
        new_listings: dataResult["md:new_listings"],
        inventory: dataResult["md:inventory"],
        median_dom: dataResult["md:median_dom"],
        off_market_in_two_weeks: dataResult["md:off_market_in_two_weeks"]
      });
      res.send(html);
    });
});
app.use(express.static("public"));

/* Send simulated weather to kafka */
var kafka = require("kafka-node");
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({ kafkaHost: process.argv[5] });
var kafkaProducer = new Producer(kafkaClient);

app.get("/weather.html", function (req, res) {
  var station_val = req.query["station"];
  var fog_val = req.query["fog"] ? true : false;
  var rain_val = req.query["rain"] ? true : false;
  var snow_val = req.query["snow"] ? true : false;
  var hail_val = req.query["hail"] ? true : false;
  var thunder_val = req.query["thunder"] ? true : false;
  var tornado_val = req.query["tornado"] ? true : false;
  var report = {
    station: station_val,
    clear:
      !fog_val &&
      !rain_val &&
      !snow_val &&
      !hail_val &&
      !thunder_val &&
      !tornado_val,
    fog: fog_val,
    rain: rain_val,
    snow: snow_val,
    hail: hail_val,
    thunder: thunder_val,
    tornado: tornado_val,
  };

  kafkaProducer.send(
    [{ topic: "weather-reports", messages: JSON.stringify(report) }],
    function (err, data) {
      console.log(err);
      console.log(report);
      res.redirect("submit-weather.html");
    }
  );
});

app.listen(port);
