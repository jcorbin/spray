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

var keyStreams = LRU({
    max: 200,
    dispose: function(key, stream) {stream.close();}
});

var ensuredDirs = {};

function outStream(key, done) {
    var stream = keyStreams.get(key);
    if (stream !== undefined) return done(null, stream);
    var keyPath = path.join.apply(path, key);
    var outPath = path.join(outBase, keyPath);
    var dirPath = path.dirname(outPath);
    var finish = under(done, function() {
        ensuredDirs[dirPath] = true;
        stream = fs.createWriteStream(outPath, {flags: 'a'});
        keyStreams.set(key, stream);
        retrun stream;
    });
    if (ensuredDirs[dirPath]) {
        finish();
    } else {
        mkdirp(dirPath, finish);
    }
}

process.stdin
    .pipe(linestream())
    .pipe(through2.obj(function(line, enc, done) {
        safeParse(line, under(done, this.push.bind(this)));
    }))
    .pipe(through2.obj(function(record, enc, done) {
        outStream(getKey(record), under(done, function(stream) {
            stream.write(JSON.stringify(record) + '\n');
        }));
    }))
    ;

function under(done, func) {
    return function(err) {
        if (err) return done(err);
        var result = func.apply(this, arguments);
        done(null, result);
    };
}
