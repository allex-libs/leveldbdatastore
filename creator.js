function createDataStore (execlib, leveldblib, jobondestroyablelib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib,
    jobs = require('./jobs')(execlib, jobondestroyablelib, leveldblib);

  function rejecter (defer) {
    defer.reject(new lib.Error('LDB_DATASTORE_DESTROYING'));
  }

  function LDBDataStore (path, outerfetchercb, outerkey2innerkeyfunc, starteddefer) {
    this.outerFetcher = null;
    this.outerkey2innerkeyFunc = null;
    this.ldb = null;
    this.ldb = this.createStorage(path, starteddefer);
    this.setOuterFetcher(outerfetchercb);
    this.setOuterKey2InnerKeyFunc(outerkey2innerkeyfunc);
    this.outerFetchDefers = new lib.Map();
    this.fetchingQ = new qlib.JobCollection();
  }
  LDBDataStore.prototype.destroy = function () {
    if (this.fetchingQ) {
      this.fetchingQ.destroy();
    }
    this.fetchingQ = null;
    if (this.outerFetchDefers) {
      this.outerFetchDefers.traverse(rejecter);
      this.outerFetchDefers.destroy;
    }
    this.outerFetchDefers = null;
    if (this.ldb) {
      this.ldb.destroy();
    }
    this.outerkey2innerkeyFunc = null;
    this.outerFetcher = null;
    this.ldb = null;
  };

  LDBDataStore.prototype.createStorage = function (path, starteddefer) {
    var mystarteddefer;
    if (starteddefer) {
      mystarteddefer = q.defer();
      mystarteddefer.promise.then(
        starteddefer.resolve.bind(starteddefer, this),
        starteddefer.reject.bind(starteddefer)
      );
    }
    return new leveldblib.LevelDBHandler({
      dbname: path,
      dbcreationoptions: {
        valueEncoding: 'json'
      },
      starteddefer: mystarteddefer
    })
  };

  LDBDataStore.prototype.setOuterFetcher = function (outerfetchercb) {
    this.outerFetcher = outerfetchercb;
  };

  LDBDataStore.prototype.setOuterKey2InnerKeyFunc = function (outerkey2innerkeyfunc) {
    this.outerkey2innerkeyFunc = outerkey2innerkeyfunc;
  };

  LDBDataStore.prototype.fetch = function (keys) {
    return (new this.jobs.FetcherJob(this, keys)).go();
    //return this.fetchingQ.run('.', new this.jobs.FetcherJob(this, keys));
  };

  LDBDataStore.prototype.removeSelfKeys = function (keys) {
    return this.fetchingQ.run('.', new this.jobs.SelfKeysRemoverJob(this, keys));
    //return (new this.jobs.SelfKeysRemoverJob(this, keys)).go();
  };

  LDBDataStore.prototype.fetchAndReportMissing = function (keys, defer, index, found, missing, missingindices) {
    return this.fetchingQ.run('.', new this.jobs.FetchAndReportMissingJob(this, keys));
    //return (new this.jobs.FetchAndReportMissingJob(this, keys)).go();
  };

  LDBDataStore.prototype.toInnerKey = function (outerkey) {
    return lib.isFunction(this.outerkey2innerkeyFunc) ?
      this.outerkey2innerkeyFunc(outerkey) :
      outerkey;
  };

  LDBDataStore.prototype.onMissingFetched = function (missingfound) {
    if (!lib.isArray(missingfound)) {
      return q([]);
    }
    if (missingfound[0] === 'complex') {
      missingfound.shift();
      return this.buildTempDB(missingfound).then(
        this.drainTempDB.bind(this)
      )
    }
    if (missingfound[0] === 'joiner') {
      return this.drainTempDB(missingfound[1]);
    }
    return this.putMissing(missingfound);
  };

  LDBDataStore.prototype.putMissing = function (missingfound, defer, index) {
    var keyval, ldbkey;
    defer = defer || q.defer();
    index = index || 0;
    //console.log(index, '>=', missingfound.length, '?');
    if (index >= missingfound.length) {
      defer.resolve(missingfound);
    } else {
      keyval = missingfound[index];
      if (!lib.isArray(keyval)) {
        throw new lib.Error('MISSINGFOUND_HAS_TO_BE_AN_ARRAY', 'Missing value found has to be an array [missingkey, valuefound]');
      }
      if (keyval.length !== 2) {
        throw new lib.Error('MISSINGFOUND_HAS_TO_BE_AN_ARRAY', 'Missing value found has to be an array [missingkey, valuefound]');
      }
      //console.log('keyval?', keyval);
      ldbkey = this.toInnerKey(keyval[0]);
      //console.log('will put', keyval[1], 'as', ldbkey);
      this.ldb.put(ldbkey, keyval[1]).then(
        this.resolveFetchDeferAfterSuccessfulPut.bind(this, missingfound, defer, index, ldbkey)
      );
    }
    return defer.promise;
  };

  LDBDataStore.prototype.resolveFetchDeferAfterSuccessfulPut = function (missingfound, defer, index, ldbkey) {
    this.resolveFetchDefer(ldbkey, missingfound[index][1]);
    this.putMissing(missingfound, defer, index+1);
  };

  LDBDataStore.prototype.resolveFetchDefer = function (ldbkey, ldbval) {
    var fd = this.outerFetchDefers.remove(ldbkey);
    if (fd) {
      fd.resolve([ldbkey, ldbval]);
    }
  };
  
  LDBDataStore.prototype.resolveFetchDeferAfterSuccessfulDrain = function (ldbkeyval) {
    this.resolveFetchDefer(ldbkeyval[0], ldbkeyval[1]);
    return q(true);
  };
  
  LDBDataStore.prototype.buildTempDB = function (missingfound) {
    return (new this.jobs.BuildTempDBJob(this, missingfound)).go();
  };

  LDBDataStore.prototype.drainTempDB = function (tdb) {
    var missingfound = [], returner = qlib.returner(missingfound);
    return tdb.traverse(this.putWithResolve.bind(this, missingfound)).then(
      function () {
        var ret = tdb.drop().then(returner);
        tdb = null;
        returner = null;
        return ret;
      }
    );
  };

  LDBDataStore.prototype.putWithResolve = function (missingfound, keyval) {
    missingfound.push(this.ldb.put(keyval.key, keyval.value).then(
      this.resolveFetchDeferAfterSuccessfulDrain.bind(this)
    ));
  };

  LDBDataStore.prototype.jobs = jobs;

  return q(LDBDataStore);
}

module.exports = createDataStore;
