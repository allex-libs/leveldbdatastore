function createDataStore (execlib, leveldblib) {
  'use strict';

  var lib = execlib.lib,
    q = lib.q,
    qlib = lib.qlib;

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
  }
  LDBDataStore.prototype.destroy = function () {
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
      missing = foundandmissing.missing;
    if (!(missing && missing.length>0)) {
      return q(found);
    }
    if (!this.outerFetcher) {
      return q.reject(new lib.Error('NO_OUTER_FETCHER_CB', 'Cannot fecher outer data without an outerFetcher function'));
    }
    return this.outerFetcher(missing).then(
      this.putMissing.bind(this)
    ).then(
      this.resolveMissingFound.bind(this, found, missing)
    );
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

  LDBDataStore.prototype.resolveMissingFound = function (found, missing, missingfound) {
    //check if all the missings are resolved in missingfound
    return q(found.concat(missingfound));
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
        this.putMissing.bind(this, missingfound, defer, index+1)
      );
    }
    return defer.promise;
  };

  return q(LDBDataStore);
}

module.exports = createDataStore;
