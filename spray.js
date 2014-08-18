var LRU = require('lru-cache');
var fs = require('fs');
var linestream = require('line-stream');
var mkdirp = require('mkdirp');
var path = require('path');
var safeParse = require('safe-json-parse');
var through2 = require('through2');

var SprayStream = require('./lib/spray_stream');

var args = require('minimist')(process.argv.slice(2), {
    boolean: ['prune'],
    alias: {
        outdir: 'o',
        prune: 'p'
    }
});

var prune = args.prune;
var outBase = args.outdir;
var fields = args._;

var n = fields.length;

function getKey(record) {
    var key = new Array(n);
    for (var i=0; i<n; i++) {
        var field = fields[i];
        key[i] = '' + record[field];
        if (prune) delete record[field];
    }
    return key;
}

var ensuredDirs = {};

function createDerpStream(outPath, opts, done) {
    var dirPath = path.dirname(outPath);
    var finish = under(done, function() {
        ensuredDirs[dirPath] = true;
        return fs.createWriteStream(outPath, opts);
    });
    if (!ensuredDirs[dirPath]) return mkdirp(dirPath, finish);
    finish();
}

function wrapStream(stream) {
    var ser = through2.obj(function(obj, _, done) {
        this.push(JSON.stringify(obj) + '\n');
        done();
    });
    ser.pipe(stream);
    return ser;
}

function openKeyStream(key, done) {
    var keyPath = path.join.apply(path, key);
    var outPath = path.join(outBase, keyPath);
    createDerpStream(outPath, {flags: 'a'}, done);
}

var keyStreams = LRU({
    max: 200,
    dispose: function(key, stream) {stream.close();}
});

process.stdin
    .pipe(linestream())
    .pipe(through2.obj(function(line, enc, done) {
        safeParse(line, under(done, this.push.bind(this)));
    }))
    .pipe(SprayStream(function(record, done) {
        var key = getKey(record);
        var stream = keyStreams.get(key);
        if (stream !== undefined) return done(null, stream);
        openKeyStream(key, under(done, function(stream) {
            stream = wrapStream(stream);
            keyStreams.set(key, stream);
        }));
    }, {objectMode: true}))
    ;

function under(done, func) {
    return function(err) {
        if (err) return done(err);
        var result = func.apply(this, arguments);
        done(null, result);
    };
}
