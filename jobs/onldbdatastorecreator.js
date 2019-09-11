function createOnLDBDataStoreJob (lib, jobondestroyablelib, mylib) {
  'use strict';

  var JobOnDestroyableBase = jobondestroyablelib.JobOnDestroyableBase;

  function JobOnLDBDataStore (ds, defer) {
    JobOnDestroyableBase.call(this, ds, defer);
  }
  lib.inherit(JobOnLDBDataStore, JobOnDestroyableBase);
  JobOnLDBDataStore.prototype._destroyableOk = function () {
    if (!this.destroyable) {
      console.error(this.constructor.name+' has no LDBDataStore, cannot continue');
      return false;
    }
    if (!this.destroyable.outerFetcher) {
      console.error('LDBDataStore of '+this.constructor.name+' has no outerFetcher, cannot continue');
      return false;
    }
    if (!this.destroyable.ldb) {
      console.error('LDBDataStore of '+this.constructor.name+' has no LevelDBHandler, cannot continue');
      return false;
    }
    return true;
  };

  mylib.JobOnLDBDataStore = JobOnLDBDataStore;
}

module.exports = createOnLDBDataStoreJob;
