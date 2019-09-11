function createBuildTempDBJob (lib, jobondestroyablelib, leveldblib, mylib) {
  'use strict'

  var q = lib.q,
    qlib = lib.qlib,
    JobOnDestroyableBase = jobondestroyablelib.JobOnDestroyableBase,
    JobOnLDBDataStore = mylib.JobOnLDBDataStore;

  function BuildTempDBJob (ds, missingfound, defer) {
    JobOnLDBDataStore.call(this, ds, defer);
    this.missingfound = missingfound;
    this.tempdb = null;
  }
  lib.inherit(BuildTempDBJob, JobOnLDBDataStore);
  BuildTempDBJob.prototype.destroy = function () {
    //don't destroy the tempdb, it's being resolved to the outer world
    this.tempdb = null;
    this.missingfound = null;
    JobOnLDBDataStore.prototype.destroy.call(this);
  };
  BuildTempDBJob.prototype.reject = function (reason) {
    //but on reject, destroy the tempdb
    if (this.tempdb) {
      this.tempdb.destroy();
    }
    this.tempdb = null;
    JobOnLDBDataStore.prototype.reject.call(this, reason);
  };
  BuildTempDBJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    this.createTempDB();
    return ok.val;
  };
  BuildTempDBJob.prototype.createTempDB = function () {
    var sd = q.defer();
    this.tempdb = 'waiting';
    sd.promise.then(
      this.onTempDB.bind(this),
      this.reject.bind(this)
    );
    new leveldblib.LevelDBHandler({
      dbname: lib.uid()+'temp.db',
      dbcreationoptions: {
        valueEncoding: 'json'
      },
      starteddefer: sd
    })
  };
  BuildTempDBJob.prototype.onTempDB = function (tdb) {
    if (!this.okToProceed) {
      return;
    }
    this.tempdb = tdb;
    (new qlib.PromiseExecutorJob(this.missingfound.map(this.updateTempDBProc.bind(this)))).go().then(
      this.onUpdateDone.bind(this),
      this.reject.bind(this)
    );
  };
  BuildTempDBJob.prototype.updateTempDBProc = function (missingfoundbatch) {
    var job = new UpdateTempDBJob(this.destroyable, this.tempdb, missingfoundbatch);
    return job.go.bind(job);
  };
  BuildTempDBJob.prototype.onUpdateDone = function () {
    this.resolve(this.tempdb);
  };

  function UpdateTempDBJob (ds, db, missingfound) {
    JobOnDestroyableBase.call(this, {ds: ds, db: db}, missingfound);
    this.missingfound = missingfound;
    this.index = 0;
  }
  lib.inherit(UpdateTempDBJob, JobOnDestroyableBase);
  UpdateTempDBJob.prototype.destroy = function () {
    this.index = null;
    this.missingfound = null;
    JobOnDestroyableBase.prototype.destroy.call(this);
  };
  UpdateTempDBJob.prototype._destroyableOk = function () {
    if (!this.destroyable) {
      console.error(this.constructor.name+' is destroyed, cannot continue');
      return false;
    }
    if (!JobOnLDBDataStore.prototype._destroyableOk.call({destroyable: this.destroyable.ds})) {
      return false;
    }
    if (!this.destroyable.db) {
      console.error(this.constructor.name+' has no DB, cannot continue');
      return false;
    }
    if (!this.destroyable.db.dbname) {
      console.error(this.constructor.name+' has a destroyed DB, cannot continue');
      return false;
    }
    return true;
  };
  UpdateTempDBJob.prototype.go = function () {
    var ok = this.okToGo();
    if (!ok.ok) {
      return ok.val;
    }
    if (this.index !== 0) {
      return ok.val;
    }
    this.update();
    return ok.val;
  };
  UpdateTempDBJob.prototype.update = function () {
    var keyval, ldbkey, ldbval;
    if (this.index >= this.missingfound.length) {
      this.resolve(true);
      return;
    }
    keyval = this.missingfound[this.index];
    ldbkey = this.destroyable.ds.toInnerKey(keyval[0]);
    ldbval = keyval[1];
    this.destroyable.db.upsert(ldbkey, extender.bind(null, ldbval), {}).then(
      this.onUpserted.bind(this),
      this.reject.bind(this)
    );
    ldbval = null;
  };
  UpdateTempDBJob.prototype.onUpserted = function () {
    if (!this.okToGo()) {
      return;
    }
    this.index++;
    this.update();
  }

  function extender (value, foundrecord) {
    lib.extend(foundrecord, value);
    value = null;
    return foundrecord;
  }


  mylib.BuildTempDBJob = BuildTempDBJob;
}

module.exports = createBuildTempDBJob;
