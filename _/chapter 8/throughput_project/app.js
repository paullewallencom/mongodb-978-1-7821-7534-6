var fs = require('fs');
var util = require('util');
var mongo = require('mongodb').MongoClient;
var assert = require('assert');

// Connection URL
var url = 'mongodb://192.168.59.103:27001/test';
// Create the date object and set hours, minutes,
// seconds and milliseconds to 00:00:00.000
var today = new Date();
today.setHours(0, 0, 0, 0);

var logDailyHit = function(db, resource, callback){
  // Get the events collection
  var collection = db.collection('events');
  // Update daily stats
  collection.update({resource: resource, date: today},
    {$inc : {daily: 1}}, {upsert: true},
    function(error, result){
      assert.equal(error, null);
      assert.equal(1, result.result.n);
      console.log("Daily Hit logged");
      callback(result);
  });
}

var logMinuteHit = function(db, resource, callback) {
  // Get the events collection
  var collection = db.collection('events');
  // Get current minute to update
  var currentDate = new Date();
  var minute = currentDate.getMinutes();
  var hour = currentDate.getHours();
  // We calculate the minute of the day
  var minuteOfDay = minute + (hour * 60);
  var minuteField = util.format('minute.%s', minuteOfDay);
  // Create a update object
  var update = {};
  var inc = {};
  inc[minuteField] = 1;
  update['$inc'] = inc;

  // Update minute stats
  collection.update({resource: resource, date: today},
    update, {upsert: true}, function(error, result){
      assert.equal(error, null);
      assert.equal(1, result.result.n);
      console.log("Minute Hit logged");
      callback(result);
  });
}

var preAllocate = function(db, resource, callback){
  // Get the events collection
  var collection = db.collection('events');
  var now = new Date();
  now.setHours(0,0,0,0);
  // Create the minute document
  var minuteDoc = {};
  for(i = 0; i < 1440; i++){
    minuteDoc[i] = 0;
  }
  // Update minute stats
  collection.update(
      {resource: resource,
        date: now,
        daily: 0},
      {$set: {minute: minuteDoc}},
      {upsert: true}, function(error, result){
      assert.equal(error, null);
      assert.equal(1, result.result.n);
      console.log("Pre-allocated successfully!");
      callback(result);
  });
}

var getCurrentDayhitStats = function(db, resource, callback){
  // Get the events collection
  var collection = db.collection('events');
  var now = new Date();
  now.setHours(0,0,0,0);
  collection.findOne({resource: "/", date: now},
    {daily: 1}, function(err, doc) {
    assert.equal(err, null);
    console.log("Document found.");
    console.dir(doc);
    callback(doc);
  });
}

var getAverageRequestPerMinuteStats = function(db, resource, callback){
  // Get the events collection
  var collection = db.collection('events');
  var now = new Date();
  // get hours and minutes and hold
  var hour = now.getHours()
  var minute = now.getMinutes();
  // calculate minute of the day to get the avg
  var minuteOfDay = minute + (hour * 60);
  // set hour to zero to put on criteria
  now.setHours(0, 0, 0, 0);
  // create the project object and set minute of the day value
  collection.findOne({resource: resource, date: now},
    {daily: 1}, function(err, doc) {
    assert.equal(err, null);
    console.log("The avg rpm is: "+doc.daily / minuteOfDay);
    console.dir(doc);
    callback(doc);
  });
}

var getBetweenDatesDailyStats = function(db, resource, dtFrom, dtTo, callback){
  // Get the events collection
  var collection = db.collection('events');
  // set hours for date parameters
  dtFrom.setHours(0,0,0,0);
  dtTo.setHours(0,0,0,0);
  collection.find({date:{$gte: dtFrom, $lte: dtTo}, resource: resource},
    {date: 1, daily: 1},{sort: [['date', 1]]}).toArray(function(err, docs) {
    assert.equal(err, null);
    console.log("Documents founded.");
    console.dir(docs);
    callback(docs);
  });
}

var getCurrentMinuteStats = function(db, resource, callback){
  // Get the events collection
  var collection = db.collection('events');
  var now = new Date();
  // get hours and minutes and hold
  var hour = now.getHours()
  var minute = now.getMinutes();
  // calculate minute of the day to create fied name
  var minuteOfDay = minute + (hour * 60);
  var minuteField = util.format('minute.%s', minuteOfDay);
  // set hour to zero to put on criteria
  now.setHours(0, 0, 0, 0);
  // create the project object and set minute of the day value
  var project = {};
  project[minuteField] = 1;
  collection.findOne({resource: "/", date: now},
    project, function(err, doc) {
    assert.equal(err, null);
    console.log("Document found.");
    console.dir(doc);
    callback(doc);
  });
}

// Connect to MongoDB and log
mongo.connect(url, function(err, db) {
  assert.equal(null, err);
  console.log("Connected to server");
  var resource = "/";
  getCurrentDayhitStats(db, resource, function(){
    getCurrentMinuteStats(db, resource, function(){
      getAverageRequestPerMinuteStats(db, resource, function(){
        var now = new Date();
        var yesterday = new Date(now.getTime());
        yesterday.setDate(now.getDate() -1);
        getBetweenDatesDailyStats(db, resource, yesterday, now, function(){
          db.close();
          console.log("Disconnected from server");
        });

      });
    });
  });
});
