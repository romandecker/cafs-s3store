/**
 * @author Roman Decker
 * Open sourced with permission from innovation.rocks consulting gmbh
 */

'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const log = require('debug')('cafs:s3store');
const stream = require('stream');

/**
 * A store for cafs which will store all blobs in amazon S3
 *
 * @param {Object} options S3Store options
 * @param {Store} options.s3 S3 instance as created by aws-sdk
 * @param {String} options.bucket The S3 bucket to use for storage
 * @param {Function} [options.log] Custom logging function (should behave like `console.log()`)
 *
 * @return {CacheStore} A new S3Store
 */
function S3Store(options) {
  options = _.defaults({}, options, { log });
  this.s3 = Promise.promisifyAll(options.s3);
  this.bucket = options.bucket;
  this.log = options.log;
}

/**
 * Store the given stream under the given file name in the store's bucket.
 *
 * @param {String} key file name
 * @param {Stream} sourceStream The source stream to store
 * @return {Promise<>} Resolves when storing process has finished.
 */
// must export
S3Store.prototype.ensure = function(key, sourceStream) {
  // must use pass-through here, because aws-sdk uses the readable event, and other listeners for
  // "data" might have possible already consumed the readable data after the event
  const pass = new stream.PassThrough();
  sourceStream.pipe(pass);

  sourceStream.on('data', data => log('Uploading %d bytes to s3', data.length));
  sourceStream.on('end', () => log('Finalizing s3-upload'));

  log('Creating s3-upload');
  return this.s3
    .uploadAsync({
      Bucket: this.bucket,
      Key: key,
      Body: pass
    })
    .tap(resp => log('S3-Upload completed successfully: %o', resp));
};

/**
 * Streams the given key out of S3 to the given destination. Options will be passed to
 * `stream.pipe()`.
 *
 * @param {String} key file name
 * @param {Writable} dest The destination stream to pipe to
 * @return {Promise<>} Resolves when streaming process has finished.
 */
// must export
S3Store.prototype.stream = function(key, dest, options) {
  const request = this.s3.getObject({
    Bucket: this.bucket,
    Key: key
  });

  return new Promise(function(resolve, reject) {
    const rs = request.createReadStream();
    rs.pipe(dest, options);

    request.on('error', reject);
    rs.on('error', reject);
    rs.on('end', resolve);
  });
};

/**
 * Move source to dest within the configured bucket.
 * @param {String} source file name
 * @param {Stream} dest file name
 * @return {Promise<>} Resolves when moving process has finished
 */
// must export
S3Store.prototype.move = function(source, dest) {
  return this.copy(source, dest).then(() =>
    this.s3.deleteObjectAsync({
      Bucket: this.bucket,
      Key: source
    })
  );
};

/**
 * Copy source to dest within the configured bucket. This is just for convenience caller is
 * responsible for managing the copied file afterwards!
 *
 * @param {String} source file name
 * @param {Stream} dest file name
 * @return {Promise<>} Resolves when copying process has finished
 */
S3Store.prototype.copy = function(source, dest, options = {}) {
  return this.s3.copyObjectAsync({
    Bucket: this.bucket,
    CopySource: this.bucket + '/' + source,
    Key: dest,
    ACL: options.acl
  });
};

/**
 * Checks whether the file at `key` exists in the configured bucket
 *
 * @param {String} key file name
 * @return {Promise<Boolean>} True if the file exists, otherwise false
 */
// must export
S3Store.prototype.exists = function(key) {
  return this.s3
    .getObjectAsync({
      Bucket: this.bucket,
      Key: key
    })
    .thenReturn(true)
    .catch(e => e.message === 'Object does not exist', () => false);
};

/**
 * Remove the file with the given name from the configured bucket
 *
 * @param {String} key file name
 * @return {Promise<>} Resolves when removal process has finished.
 */
// must export
S3Store.prototype.unlink = function(key) {
  return this.s3.deleteObjectAsync({
    Bucket: this.bucket,
    Key: key
  });
};

/**
 * Simply proxy to `s3.putObjectAcl` and resolve with a promise.
 *
 * @param {String} key key
 * @param {Object} ACL see AWS docs for putObjectAcl
 * @return {Promise<Any>} Whatever s3.putObject passes to its callback
 */
S3Store.prototype.putAcl = function(key, acl) {
  return this.s3.putObjectAclAsync({
    Bucket: this.bucket,
    Key: key,
    ACL: acl
  });
};

/**
 * Get the full S3-path (including the bucket name) for the given key
 *
 * @param {String} key Key of the object whose full s3 path to obtain
 * @return {String} The full S3 path including the bucket
 */
S3Store.prototype.getFullS3Path = function(key) {
  return `/${this.bucket}/${key}`;
};

module.exports = S3Store;
