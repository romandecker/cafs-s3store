/**
 * @author Roman Decker
 * Open sourced with permission from innovation.rocks consulting gmbh
 */
const S3 = require('mock-s3').S3;
const wrapper = require('mock-s3/lib/wrapper.js');
const Bucket = require('mock-s3/lib/bucket.js');
const Request = require('mock-s3/lib/request.js');
const streamBuffers = require('stream-buffers');

S3.prototype.copyObject = wrapper('copyObject', function(options) {
  return this._buckets[options.Bucket].copyObject(options);
});

S3.prototype.upload = function(options, callback) {
  const buf = new streamBuffers.WritableStreamBuffer();

  options.Body.pipe(buf);

  options.Body.on('end', () => {
    return this.putObject(
      {
        Bucket: options.Bucket,
        Key: options.Key,
        Body: buf.getContents()
      },
      callback
    );
  });
};

Bucket.prototype.copyObject = function(options, callback) {
  const slashPos = options.CopySource.indexOf('/');
  const sourceBucket = options.CopySource.slice(0, slashPos);
  const sourceKey = options.CopySource.slice(slashPos + 1);

  const src = this.getFile({ Bucket: sourceBucket, Key: sourceKey });

  this.putObject(
    {
      Bucket: options.Bucket,
      Key: options.Key,
      Body: src.getData()
    },
    callback
  );
};

const originalValidate = Request.prototype._validate;

Request.prototype._validate = function() {
  if (this.methodName === 'copyObject') {
    return Promise.resolve();
  } else {
    return originalValidate.apply(this, arguments);
  }
};

module.exports = {
  createMock: () => new S3()
};
