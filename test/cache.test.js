/**
 * @author Roman Decker
 * Open sourced with permission from innovation.rocks consulting gmbh
 */
'use strict';

const _ = require('lodash');
const Promise = require('bluebird');
const cafs = require('cafs');
const uuid = require('uuid');
const path = require('path');
const os = require('os');
const fs = require('fs-extra');
const s3mock = require('./s3-mock.js');

const sinon = require('sinon');
const chai = require('chai');
const expect = chai.expect;
const S3Store = require('../index.js');
chai.use(require('chai-as-promised'));
chai.use(require('sinon-chai'));

describe('Use with CacheStore', function() {
  const base = path.join(__dirname, 'store');
  const BUCKET_NAME = 'test-bucket-c5883e23-e48d-49cf-9f15-08ad90e686d5';

  let s3, directoryStore, s3Store, cfs;

  beforeEach(function() {
    s3 = s3mock.createMock();
    directoryStore = new cafs.DirectoryStore({ base });
    s3Store = new S3Store({ s3, bucket: BUCKET_NAME });

    sinon.spy(directoryStore, 'ensure');
    sinon.spy(directoryStore, 'stream');
    sinon.spy(directoryStore, 'unlink');
    sinon.spy(s3Store, 'ensure');
    sinon.spy(s3Store, 'stream');
    sinon.spy(s3Store, 'unlink');

    cfs = cafs({
      store: new cafs.CacheStore({
        fallbackStore: s3Store,
        cacheLimit: 100 * 1024,
        cacheStore: directoryStore
      }),

      mapKey: ({ hash, meta }) => (hash ? hash + meta : `tmp/${uuid.v4()}` + meta)
    });

    return Promise.join(s3.createBucketAsync({ Bucket: BUCKET_NAME }), fs.emptyDir(base));
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
        .then(() => cfs.put(Buffer.from('hello, world!'), '.txt'))
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
        .then(() => cfs.put(fs.createReadStream(tmpPath), '.txt'))
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

          return cfs.put(Buffer.from('hello, world!'), '.txt');
        })
        .then(function(info2) {
          this.info2 = info2;
          expect(info2).to.have.property('size', 13);
          expect(info2).to.have.property('meta', '.txt');

          return cfs.readFile(info2);
        })
        .then(function(buf) {
          expect(buf.toString('utf-8')).to.equal('hello, world!');

          expect(this.info1.hash).to.equal(this.info2.hash);
        });
    });

    it('should correctly evict files from directory store', function() {
      const as = _.repeat('a', 10200);
      const bs = _.repeat('b', 20200);
      const cs = _.repeat('c', 30010);
      const ds = _.repeat('d', 46066);

      return cfs
        .put(Buffer.from(as), '.txt')
        .bind({})
        .then(function(info) {
          this.aInfo = info;

          expect(directoryStore.unlink).to.not.have.been.called;
          return cfs.put(Buffer.from(bs), '.txt');
        })
        .then(function(info) {
          this.bInfo = info;

          expect(directoryStore.unlink).to.not.have.been.called;
          return cfs.put(Buffer.from(cs), '.txt');
        })
        .then(function(info) {
          this.cInfo = info;

          expect(directoryStore.unlink).to.not.have.been.called;
          return cfs.put(Buffer.from(ds), '.txt');
        })
        .then(function(info) {
          this.dInfo = info;
          expect(directoryStore.unlink).to.have.been.called;

          return cfs.readFile(this.bInfo);
        })
        .then(function(buf) {
          expect(buf.toString()).to.equal(bs);

          expect(s3Store.stream).to.not.have.been.called;
          expect(directoryStore.stream).to.have.been.called;
        });
    });
  });

  describe('#unlink', function() {
    it('should remove a file', function() {
      return cfs
        .put(Buffer.from('hello, world!'), '.txt')
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
