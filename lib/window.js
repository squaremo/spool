/*
Moving windows using Redis.
*/

var redis = require('redis'),
  events = require('events');

function RedisContext(connectOpts) {
  events.EventEmitter.call(this);
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

  this._buffers = {};
  this._values = {};
}
RedisContext.prototype = new events.EventEmitter();

RedisContext.prototype.buffer = function(topic) {
  var buffer;
  if (buffer = this._buffers[topic]) {
    return buffer;
  }
  else {
    this._buffers[topic] = buffer = new HwmBuffer(this._client, topic);
    this._subclient.subscribe(topic);
    // race? (can a sub arrive before the result?)
    buffer.update();
    this.updates.on(topic, function() { buffer.update(); });
    return buffer;
  }
};

RedisContext.prototype.value = function(topic) {
  var signal;
  if (signal = this._values[topic]) {
    return signal;
  }
  else {
    // FIXME reusing the topic -- should be namespaced to values
    this._values[topic] = signal = new Signal(this._client, topic);
    this._subclient.subscribe(topic);
    this.updates.on(topic, function() { signal.update(); });
    return signal;
  }
}

exports.createContext = function(opts) {
  return new RedisContext(opts || {});
};


// An appender. Only append entries if they are newer than the current
// high water mark. BIG NB: assumes monotonically increasing
// timestamps.
function HwmBuffer(client, topic) {
  this._client = client;
  this._topic = topic;
  this._setkey = 'set:' + topic;
  this._hashkey = 'hash:' + topic;
  this._hwm = "-inf";
  this._count = 0;
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
      var count = 0;
      entries.forEach(function(entry) {
        if (entry.timestamp > hwm) {
          count++;
          multi.hset(hash, entry.id, JSON.stringify(entry));
          multi.zadd(set, entry.timestamp, entry.id);
        }
      });
      if (count > 0) {
        multi.publish(topic, "update");
      }
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
    function(_, idsAndScores) {
      var len = idsAndScores && idsAndScores.length;
      if (len) {
        that._hwm = idsAndScores[len - 1];
        that._count += len / 2;
        var ids = [];
        for (var i = 0; i < len; i += 2) {
          ids.push(idsAndScores[i]);
        }
        that._join(ids, function(entries) {
          that.emit('update', entries);
        });
      }
    });
};

HwmBuffer.prototype._join = function(ids, callback) {
  this._client.hmget(this._hashkey, ids, function(_, items) {
    if (items) {
      for (var j = 0; j < items.length; j++) {
        items[j] = JSON.parse(items[j]);
      }
      callback(items);
    }
  });
};

// === Queries
//
// Ask for a set of entries, and to be told when new entries arrive.
// To avoid repeating or missing entries, we answer based on what the
// buffer 'knows' now, and register for subsequent updates.

// FIXME should the event handlers be registered in the continuations?

// In ascending order, starting from and including timestamp `since`
// (or the very start if `since` is falsey).
HwmBuffer.prototype.since = function(since, callback) {
  since = since || '-inf';
  var that = this;
  var client = this._client;
  client.zrangebyscore(this._setkey, since, this._hwm, function(_, ids) {
    that._join(ids, callback);
  });
  this.on('update', function(added) {
    callback(added);
  });
};

// In ascending order, starting as many as limit back from the most
// recent.
HwmBuffer.prototype.last = function(limit, callback) {
  var client = this._client;
  var hwm = this._hwm;
  var that = this;
  client.zrangebyscore(this._setkey, '-inf', hwm,
                       'LIMIT', Math.max(0, this._count - limit), limit,
                       function(_, ids) {
                         that._join(ids, callback);
                       });
  this.on('update', function(added) {
    callback(added);
  });
};


// A changing scalar value

function Signal(client, topic) {
  events.EventEmitter.call(this);
  this._valuekey = 'value:' + topic;
  this._subkey = topic;
  this._client = client;
  this._value = null;
}
Signal.prototype = new events.EventEmitter();

Signal.prototype.write = function(data) {
  var client = this._client;
  var that = this;
  function attempt() {
    client.watch(that._valuekey);
    var multi = client.multi();
    multi.set(that._valuekey, data);
    multi.publish(that._subkey, "update");
    multi.exec(function(_, res) {
      if (res === null) attempt();
    });
  }
  attempt();
};

Signal.prototype.update = function() {
  var client = this._client;
  var that = this;
  client.get(this._valuekey, function(_, value) {
    if (value !== that._value) {
      that._value = value;
      that.emit('update', value);
    }
  });
};

// TODO Do I want this, to use a cached value, and/or to trigger an
// update on construction?
// FIXME Glitching.
Signal.prototype.read = function(callback) {
  var client = this._client;
  client.get(this._valuekey, function(_, value) {
    callback(value);
  });
  this.on('update', function(value) {
    callback(value);
  });
};

// FIXME at this point updates and read can glitch (similar is true
// for buffers).
