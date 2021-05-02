function createFetcherJob (lib, mylib) {
  'use strict';

  var JobOnLDBDataStore = mylib.JobOnLDBDataStore,
    q = lib.q;

  function FetcherJob (ds, keys, defer) {
    JobOnLDBDataStore.call(this, ds, defer);
    this.keys = keys;
    this.missingdefers = null;
    this.found = null;
    this.missingindices = null;
  }
  lib.inherit(FetcherJob, JobOnLDBDataStore);
  FetcherJob.prototype.destroy = function () {
    this.missingindices = null;
    this.found = null;
    this.missingdefers = null;
    this.keys = null;
    JobOnLDBDataStore.prototype.destroy.call(this);
  };
  FetcherJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    this.destroyable.fetchAndReportMissing(this.keys).then(
      this.onFetchAndReportMissing.bind(this),
      this.reject.bind(this)
    );
    return ok.val;
  };
  FetcherJob.prototype.onFetchAndReportMissing = function (foundandmissing) {
    if (!this.okToProceed()) {
      return;
    }
    if (!(
      foundandmissing.missing &&
      lib.isArray(foundandmissing.missing.promises) && 
      foundandmissing.missing.promises.length>0
    )) {
      this.resolve(foundandmissing.found);
      return;
    }
    this.found = foundandmissing.found;
    this.missingdefers = foundandmissing.missing.defers;
    this.missingindices = foundandmissing.missing.indices;
    if (lib.isArray(foundandmissing.tofetch) && foundandmissing.tofetch.length>0) {
      this.destroyable.outerFetcher(foundandmissing.tofetch).then(
        this.onMissingFetched.bind(this),
        this.onMissingFetchFailed.bind(this),
        this.notify.bind(this)
      );
    }
    q.all(foundandmissing.missing.promises).then(
      this.foundNmissingJoiner.bind(this),
      this.reject.bind(this)
    );
  };
  FetcherJob.prototype.onMissingFetched = function (missingfound) {
    if (!this.okToProceed()) {
      return;
    }
    this.destroyable.onMissingFetched(missingfound);
  };
  FetcherJob.prototype.onMissingFetchFailed = function (err) {
    this.missingdefers.forEach(missingdeferrejecter.bind(null, err));
    err = null;
  };
  FetcherJob.prototype.foundNmissingJoiner = function (missingfound) {
    var i;
    if (!(lib.isArray(this.missingindices) && lib.isArray(missingfound) && this.missingindices.length===missingfound.length)) {
      return q.reject(new lib.Error('MISSING_FOUND_LENGTH_MISMATCH', 'Originally missing items: '+this.missingindices.length+', found: '+missingfound.length+', this is a mismatch!'));
    }
    for (i=0; i<this.missingindices.length; i++) {
      this.found[this.missingindices[i]] = missingfound[i];
    }
    this.resolve(this.found);
  }


  function missingdeferrejecter (err, missingdefer) {
    missingdefer.reject(err);
  }

  mylib.FetcherJob = FetcherJob;
}

module.exports = createFetcherJob;
