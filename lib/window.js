/*
Moving windows using Redis to keep track.
*/

var redis = require('redis'),
  events = require('events');

function RedisContext(connectOpts) {
  var that = this;
  var client = this._client = redis.createClient(
    connectOpts.port,
    connectOpts.host,
    connectOpts);
  var subclient = this._subclient = redis.createClient(
    connectOpts.port,
    connectOpts.host,
    connectOpts);
  var latch = 2;
  function decr() {
    if (!(--latch)) {
      that.emit('ready');
    }
  }
  client.on('ready', decr);
  subclient.on('ready', decr);

  var updates = this.updates = new events.EventEmitter();
  subclient.on('message', function(channel) {
    updates.emit(channel);
  });
}
RedisContext.prototype = new events.EventEmitter();

RedisContext.prototype.createBuffer = function(topic) {
  var buffer = new HwmBuffer(this._client, topic);
  this._subclient.subscribe(topic);
  this.updates.on(topic, function() { buffer.update(); });
  return buffer;
};

exports.createContext = function(opts) {
  return new RedisContext(opts || {});
};

// An appender. Only append entries if they are newer than the current
// high water mark.
function HwmBuffer(client, topic) {
  this._client = client;
  this._topic = topic;
  this._setkey = 'set:' + topic;
  this._hashkey = 'hash:' + topic;
  this._hwm = "-inf";
}
HwmBuffer.prototype = new events.EventEmitter();

// entry :: {timestamp: t, id: i, ...}
HwmBuffer.prototype.append = function(entries) {
  var client = this._client;
  var topic = this._topic;
  var set = this._setkey;
  var hash = this._hashkey;

  function attempt() {
    client.watch(set);
    client.zrange(set, "-1", "-1", "WITHSCORES", function(_, top) {
      var hwm = (top && top.length > 1) ? top[1] : 0;
      var multi = client.multi();
      entries.forEach(function(entry) {
        if (entry.timestamp > hwm) {
          multi.hset(hash, entry.id, JSON.stringify(entry));
          multi.zadd(set, entry.timestamp, entry.id);
        }
      });
      multi.publish(topic, "update");
      multi.exec(function(_err, reply) {
        // null multibulk reply means the transaction aborted
        if (reply === null) {
          attempt();
        }
      });
    });
  };
  attempt();
};

HwmBuffer.prototype.update = function() {
  var that = this;
  var client = this._client;
  var hwm = this._hwm;
  client.zrangebyscore(
    that._setkey, '(' + hwm, "+inf", "WITHSCORES",
    function(_, newEntries) {
      if (newEntries && newEntries.length > 0) {
        that._hwm = newEntries[newEntries.length-1];
        var added = [];
        for (var i = 0; i < newEntries.length; i += 2) {
          added.push(newEntries[i]);
        }
        client.hmget(that._hashkey, added, function(_, items) {
          for (var j = 0; j < items.length; j++) {
            items[j] = JSON.parse(items[j]);
          }
          that.emit('update', items);
        });
      }
    });
};
