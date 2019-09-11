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
  }
  LDBDataStore.prototype.destroy = function () {
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
  };

  LDBDataStore.prototype.fetchAndReportMissing = function (keys, defer, index, found, missing, missingindices) {
    return (new this.jobs.FetchAndReportMissingJob(this, keys)).go();
    /*
    var innerkey;
    defer = defer || q.defer();
    index = index || 0;
    found = found || [];
    missing = missing || [];
    missingindices = missingindices || [];
    if (index >= keys.length) {
      defer.resolve({found: found, missing: missing, missingindices: missingindices});
    } else {
      innerkey = this.toInnerKey(keys[index]);
      this.ldb.safeGet(innerkey, null).then(
        this.onSingleFetchedForReportMissing.bind(this, keys, defer, index, found, missing, missingindices, innerkey),
        defer.reject.bind(defer)
      );
    }
    return defer.promise;
    */
  };

  LDBDataStore.prototype.toInnerKey = function (outerkey) {
    return lib.isFunction(this.outerkey2innerkeyFunc) ?
      this.outerkey2innerkeyFunc(outerkey) :
      outerkey;
  };

  /*
  LDBDataStore.prototype.onSingleFetchedForReportMissing = function (keys, defer, index, found, missing, missingindices, innerkey, dbval) {
    var key = keys[index];
    if (dbval===null) {
      //console.log(key, 'not found');
      found.push(null);
      missing.push(key);
      missingindices.push(index);
    } else {
      if (!lib.isString(key)) {
        console.log('zasto ovde ide Object?', key);
      }
      found.push([innerkey, dbval]);
    }
    this.fetchAndReportMissing(keys, defer, index+1, found, missing, missingindices);
  };
  */

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
    //console.log('resolveFetchDeferAfterSuccessfulPut', missingfound, index);
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

  /*
  LDBDataStore.prototype.buildTempDB = function (missingfound) {
    var sd = q.defer(),
      ret,
      tdb;
    ret = sd.promise.then(
      this.populateTempDB.bind(this, missingfound)
    );
    tdb = new leveldblib.LevelDBHandler({
      dbname: lib.uid()+'temp.db',
      dbcreationoptions: {
        valueEncoding: 'json'
      },
      starteddefer: sd
    });
    return ret;
  };

  LDBDataStore.prototype.populateTempDB = function (missingfound, tdb) {
    return (new qlib.PromiseChainerJob(missingfound.map(this.updateTempDBProc.bind(this, tdb)))).go();
  };

  LDBDataStore.prototype.updateTempDBProc = function (tdb, missingfoundbatch) {
    return this.updateTempDB.bind(this, tdb, missingfoundbatch, null, 0);
  };

  LDBDataStore.prototype.updateTempDB = function (tdb, missingfoundbatch, defer, index) {
    var keyval, ldbkey, ldbval, extend;
    defer = defer || q.defer();
    index = index || 0;
    if (index >= missingfoundbatch.length) {
      defer.resolve(tdb);
    } else {
      keyval = missingfoundbatch[index];
      ldbkey = this.toInnerKey(keyval[0]);
      ldbval = keyval[1];
      extend = lib.extend;
      //console.log('will put', keyval[1], 'as', ldbkey);
      tdb.upsert(ldbkey, function (record) {
        //console.log('extend', record, 'with', ldbval);
        extend(record, ldbval);
        extend = null;
        ldbval = null;
        return record;
      }, {}).then(
        this.updateTempDB.bind(this, tdb, missingfoundbatch, defer, index+1)
      );
    }
    return defer.promise;
  };
  */

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
