var Transform = require('stream').Transform;
var inherits = require('util').inherits;

module.exports = SprayStream;

function SprayStream(streamFunc, options) {
    if (!(this instanceof SprayStream)) {
        return new SprayStream(streamFunc, options);
    }
    if (typeof streamFunc === 'object') {
        options = streamFunc;
        streamFunc = undefined;
    }
    options = options || {};
    streamFunc = streamFunc || options.streamFunc;
    if (!streamFunc) throw new Error('no streamFunc defined');
    this.streamFunc = streamFunc;
    Transform.call(this, options);
}

inherits(SprayStream, Transform);

SprayStream.prototype._transform = function(chunk, enc, done) {
    this.streamFunc(chunk, function(err, stream) {
        if (err) return done(err);
        stream.write(chunk, enc, done);
    });
};
