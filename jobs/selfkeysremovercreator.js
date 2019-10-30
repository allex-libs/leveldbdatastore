function createSelfKeysRemoverJob (lib, mylib) {
  'use strict';

  var JobOnLDBDataStore = mylib.JobOnLDBDataStore,
    q = lib.q;

  function SelfKeysRemoverJob (ds, selfkeys, defer) {
    JobOnLDBDataStore.call(this, ds, defer);
    this.selfkeys = lib.isArray(selfkeys) ? selfkeys.slice() : [];
  }
  lib.inherit(SelfKeysRemoverJob, JobOnLDBDataStore);
  SelfKeysRemoverJob.prototype.destroy = function () {
    this.selfkeys = null;
    JobOnLDBDataStore.prototype.destroy.call(this);
  };
  SelfKeysRemoverJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    this.removeOneSelfKey();
    return ok.val;
  };
  SelfKeysRemoverJob.prototype.removeOneSelfKey = function () {
    var key;
    if (!this.okToProceed()) {
      return;
    }
    if (this.selfkeys.length<1) {
      this.resolve(true);
      return;
    }
    key = this.selfkeys.shift();
    this.destroyable.ldb.del(key).then(
      this.removeOneSelfKey.bind(this),
      this.reject.bind(this)
    );
  };

  mylib.SelfKeysRemoverJob = SelfKeysRemoverJob;
}
module.exports = createSelfKeysRemoverJob;
    
