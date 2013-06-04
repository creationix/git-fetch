var pktLine = require('git-pkt-line');
var listPack = require('git-list-pack/min.js');
var demux = require('min-stream/demux.js');
var chain = require('min-stream/chain.js');
var bops = require('bops');
var hydratePack = require('git-hydrate-pack');


module.exports = function (options, callback) {
  return function (rawRead) {

    var sources = demux(
      ["line", "pack", "progress", "error"],
      chain.pushToPull(pktLine.deframer)(rawRead)
    );
    var output = tube();
    var write = pktLine.framer(output.write);

    var refs = {};
    var caps;

    consumeTill(sources.line, function (item) {
      if (item) {
        item = decodeLine(item);
        if (item.caps) caps = item.caps;
        refs[item[1]] = item[0];
        return true;
      }
    }, function (err) {
      if (err) return write(err);
      var clientCaps = [];
      if (caps["side-band-64k"]) {
        clientCaps.push("side-band-64k");
      }
      else if (caps["side-band"]) {
        clientCaps.push("side-band");
      }
      if (caps["include-tag"]) {
        clientCaps.push("include-tag");
      }

      write(null, ["want", refs.HEAD].concat(clientCaps).join(" ") + "\n");
      write(null, null);
      write(null, "done");

      var seen = {};
      var pending = {};
      function find(oid, ready) {
        if (seen[oid]) ready(null, seen[oid]);
        else pending[oid] = ready;
      }

      callback(null, {
        caps: caps,
        refs: refs,
        objects: chain
          .source(sources.pack)
          .pull(listPack)
          .push(hydratePack(find))
          .map(function (item) {
            seen[item.hash] = item;
            if (pending[item.hash]) {
              pending[item.hash](null, item);
            }
            return item;
          }),
        line: sources.line,
        progress: sources.progress,
        error: sources.error
      });
    });

    return output;
  };
};


function consumeTill(read, check, callback) {
  read(null, onRead);
  function onRead(err, item) {
    if (item === undefined) {
      if (err) return callback(err);
      return callback();
    }
    if (!check(item)) return callback();
    read(null, onRead);
  }
}


function tube() {
  var dataQueue = [];
  var readQueue = [];
  var closed;
  function check() {
    while (!closed && readQueue.length && dataQueue.length) {
      readQueue.shift().apply(null, dataQueue.shift());
    }
  }
  function write(err, item) {
    dataQueue.push([err, item]);
    check();
  }
  function read(close, callback) {
    if (close) closed = close;
    if (closed) return callback();
    readQueue.push(callback);
    check();
  }
  read.write = write;
  return read;
}

// Decode a binary line
// returns the data array with caps and request tagging if they are found.
function decodeLine(line) {
  var result = [];

  if (line[line.length - 1] === "\0") {
    result.request = true;
    line = line.substr(0, line.length - 1);
  }
  line = line.trim();
  var parts = line.split("\0");
  result.push.apply(result, parts[0].split(" "));
  if (parts[1]) {
    result.caps = {};
    parts[1].split(" ").forEach(function (cap) {
      var pair = cap.split("=");
      result.caps[pair[0]] = pair[1] ? pair[1] : true;
    });
  }
  return result;
}

