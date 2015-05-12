var influx = require('influx');
var nodemailer = require('nodemailer');
var AWS = require('aws-sdk');
var request = require('request');
var parse = require('parse-duration');
var _ = require('lodash');

require('required_env')([
  'NAGGER_INFLUX_HOST',
  {
    var: 'NAGGER_INFLUX_PORT',
    default: '8086'
  },
  {
    var: 'NAGGER_INFLUX_PROTOCOL',
    default: 'http'
  },
  'NAGGER_INFLUX_USER',
  'NAGGER_INFLUX_PASS',
  {
    var: 'NAGGER_INTERVAL',
    default: '1m'
  },
  'NAGGER_SNS_TOPIC',
  'NAGGER_AWS_ACCESS_KEY',
  'NAGGER_AWS_SECRET',
  {
    var: 'NAGGER_SNS_REGION',
    default: 'ap-southeast-2'
  },
  'NAGGER_WATCHER_URL'
]);

var sns = new AWS.SNS({
  accessKeyId: process.env.NAGGER_AWS_ACCESS_KEY,
  secretAccessKey: process.env.NAGGER_AWS_SECRET,
  region: process.env.NAGGER_SNS_REGION
});

var createInfluxClients = function(databases) {
  var clients = {};
  databases.forEach(function(database) {
    clients[database] = influx({
      host: process.env.NAGGER_INFLUX_HOST,
      port: process.env.NAGGER_INFLUX_PORT,
      protocol: process.env.NAGGER_INFLUX_PROTOCOL,
      username: process.env.NAGGER_INFLUX_USER,
      password: process.env.NAGGER_INFLUX_PASS,
      database: database
    });
  });
  return clients;
};

var buildQueryString = function(selector, set, limit) {
  return 'SELECT ' + selector + ' AS value FROM ' + set + ' WHERE time > now() - ' + process.env.NAGGER_INTERVAL + ' AND value ' + limit + ' GROUP BY time(1s)';
};

var send_alert = function(watcher, row, point) {
  var alert = [watcher.name, 'alert!', row.name, 'measured', point[1], 'at', new Date(point[0])].join(' ');
  console.log(alert);
  var params = {
    Message: alert,
    Subject: alert,
    TopicArn: process.env.NAGGER_SNS_TOPIC
  };
  sns.publish(params, function(err, data) {
    if (err) throw err;
  });
};

var getWatchers = function(watcher_url, cb) {
  request.get(watcher_url, function(err, res, body) {
    if (err) throw err;
    cb(JSON.parse(body));
  });
};

var main_loop = function() {
  getWatchers(process.env.NAGGER_WATCHER_URL, function(watchers) {

    var clients = createInfluxClients(_.pluck(watchers, 'database'));

    watchers.forEach(function(watcher) {
      clients[watcher.database].query(buildQueryString(watcher.selector, watcher.set, watcher.limit), function(err, res) {
        if (err) console.error(err);
        res.forEach(function(row) {
          row.points.forEach(function(point) {
            send_alert(watcher, row, point);
          });
        });
      });
    });
  });
};

setInterval(main_loop, parse(process.env.NAGGER_INTERVAL));
main_loop();
