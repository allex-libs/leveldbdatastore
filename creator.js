function createDataStore (execlib, leveldblib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

  function rejecter (defer) {
    defer.reject(new lib.Error('LDB_DATASTORE_DESTROYING'));
  }

  function LDBDataStore (path, outerfetchercb, starteddefer) {
    var mystarteddefer;
    this.outerFetcher = null;
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
    this.ldb = null;
  };

  LDBDataStore.prototype.setOuterFetcher = function (outerfetchercb) {
    this.outerFetcher = outerfetchercb;
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
      this.ldb.safeGet(keys[index], null).then(
        this.onSingleFetchedForReportMissing.bind(this, keys, defer, index, found, missing)
      );
    }
    return defer.promise;
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
        this.putMissing.bind(this)
      );
    }
    return q.all(missingpromises).then(foundNmissingJoiner.bind(null, found));
  };

  LDBDataStore.prototype.decideForOuterFetch = function (missingpromises, tofetch, miss) {
    var fd = this.outerFetchDefers.get(miss);
    if (!fd) {
      fd = q.defer();
      tofetch.push(miss);
      this.outerFetchDefers.add(miss, fd);
    }
    missingpromises.push(fd.promise);
  };

  LDBDataStore.prototype.onSingleFetchedForReportMissing = function (keys, defer, index, found, missing, dbval) {
    var key = keys[index];
    if (dbval===null) {
      missing.push(key);
    } else {
      found.push([key, dbval]);
    }
    this.fetchAndReportMissing(keys, defer, index+1, found, missing);
  };

  LDBDataStore.prototype.putMissing = function (missingfound, defer, index) {
    var keyval;
    defer = defer || q.defer();
    index = index || 0;
    if (index >= missingfound.length) {
      defer.resolve(missingfound);
    } else {
      keyval = missingfound[index];
      this.ldb.put(keyval[0], keyval[1]).then(
        this.resolveFetchDeferAfterSuccessfulPut.bind(this, missingfound, defer, index)
      );
    }
    return defer.promise;
  };

  LDBDataStore.prototype.resolveFetchDeferAfterSuccessfulPut = function (missingfound, defer, index) {
    var keyval = missingfound[index];
    var fd = this.outerFetchDefers.remove(keyval[0]);
    if (fd) {
      fd.resolve(keyval);
    }
    this.putMissing(missingfound, defer, index+1);
  };
  
  function foundNmissingJoiner (found, missingfound) {
    var ret = found.concat(missingfound);
    found = null;
    return q(ret);
  }

  return q(LDBDataStore);
}

module.exports = createDataStore;
