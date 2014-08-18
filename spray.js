var LRU = require('lru-cache');
var fs = require('fs');
var linestream = require('line-stream');
var mkdirp = require('mkdirp');
var path = require('path');
var safeParse = require('safe-json-parse');
var through2 = require('through2');

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

var outStreams = LRU({
    max: 200,
    dispose: function(key, stream) {stream.close();}
});

var ensuredDirs = {};

function outStream(key, done) {
    var stream = outStreams.get(key);
    if (stream !== undefined) return done(null, stream);
    var keyPath = path.join.apply(path, key);
    var outPath = path.join(outBase, keyPath);
    var dirPath = path.dirname(outPath);
    if (ensuredDirs[dirPath]) {
        finish();
    } else {
        mkdirp(dirPath, finish);
    }
    function finish(err) {
        if (err) return done(err);
        ensuredDirs[dirPath] = true;
        stream = fs.createWriteStream(outPath, {flags: 'a'});
        outStreams.set(key, stream) ;
        done(null, stream);
    }
}

process.stdin
    .pipe(linestream())
    .pipe(through2.obj(function(line, enc, done) {
        safeParse(line, function(err, record) {
            if (err) return done(err);
            this.push(record);
            done();
        });

    }))
    .pipe(through2.obj(function(record, enc, done) {
        outStream(getKey(record), function(err, stream) {
            if (err) return done(err);
            stream.write(JSON.stringify(record) + '\n');
            done();
        });
    }))
    ;
