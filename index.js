var Writable = require('stream').Writable;
var util = require('util');
var MongoClient = require('mongodb').MongoClient;

util.inherits(StreamToMongo, Writable);

module.exports = StreamToMongo;

function StreamToMongo(options) {
  if (!(this instanceof StreamToMongo)) {
    return new StreamToMongo(options);
  }
  Writable.call(this, {
    objectMode: true
  });
  this.options = options;
}

StreamToMongo.prototype.handleDbResponse = function handleDbResponse(done, err, res) {
  if (err && err.message.match(/duplicate/)) {
    this.emit('warning', err);
    return done();
  }
  return done(err, res);
}

StreamToMongo.prototype._write = function(obj, encoding, done) {
  var self = this,
    writeOptions = {
      w: 1,
      upsert: true
    };

  if (!this.db) {
    MongoClient.connect(this.options.db, function(err, db) {
      if (err) throw err;
      self.db = db;
      self.on('finish', function() {
        self.db.close();
      });
      self.collection = db.collection(self.options.collection);
      if (self.options.overwrite) {
        self.collection.update({
          _id: obj._id
        }, obj, writeOptions, self.handleDbResponse.bind(self, done));
      } else {
        self.collection.insert(obj, writeOptions, self.handleDbResponse.bind(self, done));
      }
    });
  } else {
    if (this.options.overwrite) {
      this.collection.update({
        _id: obj._id
      }, obj, writeOptions, this.handleDbResponse.bind(this, done));
    } else {
      this.collection.insert(obj, writeOptions, this.handleDbResponse.bind(this, done));
    }
  }
};
