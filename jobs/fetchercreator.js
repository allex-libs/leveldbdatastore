function createFetcherJob (lib, mylib) {
  'use strict';

  var JobOnLDBDataStore = mylib.JobOnLDBDataStore,
    q = lib.q;

  function FetcherJob (ds, keys, defer) {
    JobOnLDBDataStore.call(this, ds, defer);
    this.keys = keys;
    this.missingdefers = null;
    this.missingpromises = null;
    this.found = null;
    this.missingindices = null;
  }
  lib.inherit(FetcherJob, JobOnLDBDataStore);
  FetcherJob.prototype.destroy = function () {
    this.missingindices = null;
    this.found = null;
    this.missingpromises = null;
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
    var missing = foundandmissing.missing,
      tofetch,
      _mps,
      _tf;
    this.found = foundandmissing.found;
    if (!(missing && missing.length>0)) {
      console.log('found all', this.found);
      this.resolve(this.found);
      return;
    }
    this.missingdefers = [];
    this.missingpromises = [];
    this.missingindices = foundandmissing.missingindices;
    tofetch = [];
    _tf = tofetch;
    missing.forEach(this.decideForOuterFetch.bind(this, _tf));
    _mps = null;
    _tf = null;
    //console.log('missingpromises', missingpromises, 'tofetch', tofetch);
    if (tofetch.length>0) {
      this.destroyable.outerFetcher(tofetch).then(
        this.onMissingFetched.bind(this),
        this.onMissingFetchFailed.bind(this),
        this.notify.bind(this)
      );
    }
    q.all(this.missingpromises).then(
      this.foundNmissingJoiner.bind(this),
      this.reject.bind(this)
    );
  };
  FetcherJob.prototype.decideForOuterFetch = function (tofetch, miss) {
    var mymiss = this.destroyable.toInnerKey(miss), fd = this.destroyable.outerFetchDefers.get(mymiss);
    if (!fd) {
      fd = q.defer();
      tofetch.push(miss);
      this.missingdefers.push(fd);
      this.destroyable.outerFetchDefers.add(mymiss, fd);
    }
    this.missingpromises.push(fd.promise);
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
