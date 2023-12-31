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

function rowToMap(row) {
  var stats = {};
  row.forEach(function (item) {
    const column = item["column"];
    stats[column] = item["$"];
  });
  return stats;
}

// hclient
//   .table("yvesyang_zip_estate")
//   .row("2022_10_60607_-1")
//   .get((error, value) => {
//     console.info(rowToMap(value));
//     // console.info(value);
//   });

app.get("/zip_data.html", function (req, res) {
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
        longitude: dataResult["md:longitude"],
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
        off_market_in_two_weeks: dataResult["md:off_market_in_two_weeks"],
      });
      res.send(html);
    });
});
app.use(express.static("public"));

app.get("/city_data.html", function (req, res) {
  const record_key =
    req.query["year"] +
    "_" +
    req.query["month"] +
    "_" +
    req.query["city"] +
    ", " +
    req.query["state"] +
    "_" +
    req.query["type"];
  console.log(record_key);

  hclient
    .table("yvesyang_city_estate")
    .row(record_key)
    .get(function (err, record) {
      const dataResult = rowToMap(record);
      console.log(dataResult);

      var template = filesystem.readFileSync("result.mustache").toString();
      var html = mustache.render(template, {
        year: req.query["year"],
        month: req.query["month"],
        type: req.query["type"],
        city: req.query["city"],
        state: req.query["state"],
        latitude: dataResult["mdc:latitude"],
        longitude: dataResult["mdc:longitude"],
        city_state: dataResult["mdc:city_state"],
        property_type: dataResult["mdc:property_type"],
        property_type_id: dataResult["mdc:property_type_id"],
        median_sale_price: dataResult["mdc:median_sale_price"],
        median_list_price: dataResult["mdc:median_list_price"],
        home_sold: dataResult["mdc:homes_sold"],
        pending_sales: dataResult["mdc:pending_sales"],
        new_listings: dataResult["mdc:new_listings"],
        inventory: dataResult["mdc:inventory"],
        median_dom: dataResult["mdc:median_dom"],
        off_market_in_two_weeks: dataResult["mdc:off_market_in_two_weeks"],
      });
      res.send(html);
    });
});

/* Send simulated weather to kafka */
var kafka = require("kafka-node");
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({ kafkaHost: process.argv[5] });
var kafkaProducer = new Producer(kafkaClient);

app.get("/market_updates.html", function (req, res) {
  var year_val = req.query["year"];
  var month_val = req.query["month"];
  var zipcode_val = req.query["zipcode"];
  var type_val = req.query["type"];
  var median_sale_price_val = req.query["median_sale_price"];
  var median_list_price_val = req.query["median_list_price"];
  var home_sold_val = req.query["home_sold"];
  var pending_sales_val = req.query["pending_sales"];
  var new_listings_val = req.query["new_listings"];
  var inventory_val = req.query["inventory"];
  var median_dom_val = req.query["median_dom"];
  var off_market_in_two_weeks_val = req.query["off_market_in_two_weeks"];

  var record_key_val =
    year_val + "_" + month_val + "_" + zipcode_val + "_" + type_val;
  var report = {
    record_key: record_key_val,
    year: year_val,
    month: month_val,
    zipcode: zipcode_val,
    property_type: type_val,
    median_sale_price: median_sale_price_val,
    median_list_price: median_list_price_val,
    home_sold: home_sold_val,
    pending_sales: pending_sales_val,
    new_listings: new_listings_val,
    inventory: inventory_val,
    median_dom: median_dom_val,
    off_market_in_two_weeks: off_market_in_two_weeks_val,
  };

  kafkaProducer.send(
    [{ topic: "yvesyang_data_updates", messages: JSON.stringify(report) }],
    function (err, data) {
      if (err) {
        console.error(err);
      }
      console.log(report);
    }
  );
});

app.listen(port);
