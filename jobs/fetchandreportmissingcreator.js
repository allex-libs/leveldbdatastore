function createFetchAndReportMissingJob (lib, mylib) {
  'use strict';

  var JobOnLDBDataStore = mylib.JobOnLDBDataStore;

  function FetchAndReportMissingJob (ds, keys, defer) {
    JobOnLDBDataStore.call(this, ds, defer);
    this.keys = keys;
    this.index = 0;
    this.found = [];
    this.missing = [];
    this.missingindices = [];
  }
  lib.inherit(FetchAndReportMissingJob, JobOnLDBDataStore);
  FetchAndReportMissingJob.prototype.destroy = function () {
    this.missingindices = null;
    this.missing = null;
    this.found = null;
    this.index = null;
    this.keys = null;
    JobOnLDBDataStore.prototype.destroy.call(this);
  };
  FetchAndReportMissingJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    if (this.index !== 0) {
      return ok.val;
    }
    this.fetchOne();
    return ok.val;
  };
  FetchAndReportMissingJob.prototype.fetchOne = function () {
    var innerkey;
    if (!this.okToProceed()) {
      return;
    }
    if (this.index >= this.keys.length) {
      this.resolve({found: this.found, missing: this.missing, missingindices: this.missingindices});
      return;
    }
    innerkey = this.destroyable.toInnerKey(this.keys[this.index]);
    this.destroyable.ldb.safeGet(innerkey, null).then(
      this.onOneFetched.bind(this, innerkey),
      this.reject.bind(this)
    );
  };
  FetchAndReportMissingJob.prototype.onOneFetched = function (innerkey, dbval) {
    var outerkey;
    if (!this.okToProceed()) {
      return;
    }
    outerkey = this.keys[this.index];
    if (dbval===null) {
      this.found.push(null);
      this.missing.push(outerkey);
      this.missingindices.push(this.index);
    } else {
      this.found.push([innerkey, dbval]);
    }
    this.index++;
    this.fetchOne();
  };

  mylib.FetchAndReportMissingJob = FetchAndReportMissingJob;
}

module.exports = createFetchAndReportMissingJob;
