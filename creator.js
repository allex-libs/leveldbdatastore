function createDataStore (execlib, leveldblib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  function rejecter (defer) {
    defer.reject(new lib.Error('LDB_DATASTORE_DESTROYING'));
  }

  function LDBDataStore (path, outerfetchercb, outerkey2innerkeyfunc, starteddefer) {
    var mystarteddefer;
    this.outerFetcher = null;
    this.outerkey2innerkeyFunc = null;
    this.ldb = null;
    if (starteddefer) {
      mystarteddefer = q.defer();
      mystarteddefer.promise.then(
        starteddefer.resolve.bind(starteddefer, this),
        starteddefer.reject.bind(starteddefer)
      );
    }
    this.ldb = new leveldblib.LevelDBHandler({
      dbname: path,
      dbcreationoptions: {
        valueEncoding: 'json'
      },
      starteddefer: mystarteddefer
    });
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

  LDBDataStore.prototype.setOuterFetcher = function (outerfetchercb) {
    this.outerFetcher = outerfetchercb;
  };

  LDBDataStore.prototype.setOuterKey2InnerKeyFunc = function (outerkey2innerkeyfunc) {
    this.outerkey2innerkeyFunc = outerkey2innerkeyfunc;
  };

  LDBDataStore.prototype.fetch = function (keys) {
    return this.fetchAndReportMissing(keys).then(
      this.onFetchAndReportMissing.bind(this)
    );
  };

  LDBDataStore.prototype.fetchAndReportMissing = function (keys, defer, index, found, missing) {
    defer = defer || q.defer();
    index = index || 0;
    found = found || [];
    missing = missing || [];
    if (index >= keys.length) {
      defer.resolve({found: found, missing: missing});
    } else {
      this.ldb.safeGet(this.toInnerKey(keys[index]), null).then(
        this.onSingleFetchedForReportMissing.bind(this, keys, defer, index, found, missing),
        defer.reject.bind(defer)
      );
    }
    return defer.promise;
  };

  LDBDataStore.prototype.toInnerKey = function (outerkey) {
    return lib.isFunction(this.outerkey2innerkeyFunc) ?
      this.outerkey2innerkeyFunc(outerkey) :
      outerkey;
  };

  LDBDataStore.prototype.onFetchAndReportMissing = function (foundandmissing) {
    console.log(foundandmissing);
    var found = foundandmissing.found,
      missing = foundandmissing.missing,
      missingpromises,
      tofetch;
    if (!(missing && missing.length>0)) {
      return q(found);
    }
    if (!this.outerFetcher) {
      return q.reject(new lib.Error('NO_OUTER_FETCHER_CB', 'Cannot fecher outer data without an outerFetcher function'));
    }
    missingpromises = [];
    tofetch = [];
    missing.forEach(this.decideForOuterFetch.bind(this, missingpromises, tofetch));
    console.log('missingpromises', missingpromises, 'tofetch', tofetch);
    if (tofetch.length>0) {
      this.outerFetcher(tofetch).then(
        this.onMissingFetched.bind(this)
      );
    }
    return q.all(missingpromises).then(foundNmissingJoiner.bind(null, found));
  };

  LDBDataStore.prototype.decideForOuterFetch = function (missingpromises, tofetch, miss) {
    var mymiss = this.toInnerKey(miss), fd = this.outerFetchDefers.get(mymiss);
    if (!fd) {
      fd = q.defer();
      tofetch.push(miss);
      this.outerFetchDefers.add(mymiss, fd);
    }
    missingpromises.push(fd.promise);
  };

  LDBDataStore.prototype.onSingleFetchedForReportMissing = function (keys, defer, index, found, missing, dbval) {
    var key = keys[index];
    if (dbval===null) {
      console.log(key, 'not found');
      missing.push(key);
    } else {
      found.push([key, dbval]);
    }
    this.fetchAndReportMissing(keys, defer, index+1, found, missing);
  };

  LDBDataStore.prototype.onMissingFetched = function (missingfound) {
    if (!lib.isArray(missingfound)) {
      return;
    }
    if (missingfound[0] === 'complex') {
      missingfound.shift();
      this.buildTempDB(missingfound).then(
        this.drainTempDB.bind(this)
      )
    } else {
      return this.putMissing(missingfound);
    }
  };

  LDBDataStore.prototype.putMissing = function (missingfound, defer, index) {
    var keyval, ldbkey;
    defer = defer || q.defer();
    index = index || 0;
    console.log(index, '>=', missingfound.length, '?');
    if (index >= missingfound.length) {
      defer.resolve(missingfound);
    } else {
      keyval = missingfound[index];
      console.log('keyval?', keyval);
      ldbkey = this.toInnerKey(keyval[0]);
      console.log('will put', keyval[1], 'as', ldbkey);
      this.ldb.put(ldbkey, keyval[1]).then(
        this.resolveFetchDeferAfterSuccessfulPut.bind(this, missingfound, defer, index, ldbkey)
      );
    }
    return defer.promise;
  };

  LDBDataStore.prototype.resolveFetchDeferAfterSuccessfulPut = function (missingfound, defer, index, ldbkey) {
    this.resolveFetchDefer(ldbkey, missingfound[index]);
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
  
  function foundNmissingJoiner (found, missingfound) {
    var ret = found.concat(missingfound);
    found = null;
    return q(ret);
  }

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

  return q(LDBDataStore);
}

module.exports = createDataStore;
