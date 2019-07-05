/**
 * @author Roman Decker
 * Open sourced with permission from innovation.rocks consulting gmbh
 */
'use strict';

const cafs = require('cafs');
const uuid = require('uuid');
const path = require('path');
const os = require('os');
const fs = require('fs-extra');
const s3mock = require('./s3-mock.js');

const chai = require('chai');
const expect = chai.expect;
const S3Store = require('../index.js');
chai.use(require('chai-as-promised'));

describe('cafs', function() {
  const BUCKET_NAME = 'test-bucket-c5883e23-e48d-49cf-9f15-08ad90e686d5';
  let s3, cfs;

  beforeEach(function() {
    s3 = s3mock.createMock();
    cfs = cafs({
      store: new S3Store({ s3, bucket: BUCKET_NAME }),
      getTemporaryKey: () => `tmp/${uuid.v4()}`
    });

    return s3.createBucketAsync({ Bucket: BUCKET_NAME });
  });

  describe('#readFile', function() {
    it('should throw an error when trying to get a non-existant file', function() {
      return expect(cfs.readFile('abc')).to.eventually.be.rejected;
    });
  });

  describe('#put', function() {
    it('should work with buffers', function() {
      return expect(cfs.hasContent(Buffer.from('hello, world!')))
        .to.eventually.equal(false)
        .then(() => cfs.put(Buffer.from('hello, world!'), 'hello.txt'))
        .tap(info => expect(cfs.has(info)).to.eventually.equal(true))
        .then(info => cfs.readFile(info))
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('hello, world!');
        });
    });

    it('should work with file-paths', function() {
      const tmpPath = path.join(os.tmpdir(), uuid.v4() + '.txt');
      return fs
        .outputFile(tmpPath, 'This is a test')
        .then(() => cfs.put(tmpPath, 'test.txt'))
        .then(info => cfs.readFile(info))
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('This is a test');
        });
    });

    it('should work with streams', function() {
      const tmpPath = path.join(os.tmpdir(), uuid.v4() + '.txt');
      return fs
        .outputFile(tmpPath, 'This is another test')
        .then(() => cfs.put(fs.createReadStream(tmpPath), 'test.txt'))
        .then(info => cfs.readFile(info))
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('This is another test');
        });
    });

    it('should correctly handle files with same content', function() {
      return cfs
        .put(Buffer.from('hello, world!'), '.txt')
        .bind({})
        .then(function(info1) {
          this.info1 = info1;
          expect(info1).to.have.property('size', 13);
          expect(info1).to.have.property('meta', '.txt');
          return cfs.readFile(info1);
        })
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('hello, world!');

          return cfs.put(Buffer.from('hello, world!'), '.md');
        })
        .then(function(info2) {
          this.info2 = info2;
          expect(info2).to.have.property('size', 13);
          expect(info2).to.have.property('meta', '.md');

          return cfs.readFile(info2);
        })
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('hello, world!');

          expect(this.info1.hash).to.equal(this.info2.hash);
          expect(this.info2.meta).to.equal(this.info2.meta);
        });
    });
  });

  describe('#unlink', function() {
    it('should remove a file', function() {
      return cfs
        .put(Buffer.from('hello, world!'), 'hello.txt')
        .bind({})
        .then(function(info) {
          this.info = info;

          return cfs.unlink(info);
        })
        .then(function() {
          return expect(cfs.has(this.info)).to.eventually.equal(false);
        });
    });
  });
});
