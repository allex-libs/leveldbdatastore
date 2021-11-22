var JobBase = qlib.JobBase;

function AwaiterJob (expectedval, defer) {
  JobBase.call(this, defer);
  this.expected = expectedval;
}
lib.inherit(AwaiterJob, JobBase);
AwaiterJob.prototype.destroy = function () {
  this.expected = null;
  JobBase.prototype.destroy.call(this);
}
AwaiterJob.prototype.go = function () {
  var ok = this.okToGo();
  if (!ok.ok) {
    return ok.val;
  }
  //nothing to do here...
  return ok.val;
};
AwaiterJob.prototype.resolve = function (val) {
  if (val !== this.expected) {
    this.reject(
      new lib.Error(
        'EXPECTED_NOT_MET', 
        'Excpected =>'+this.expected+'<= is not identical to obtained =>'+val+'<='
      )
    );
    return;
  }
  return JobBase.prototype.resolve.call(this, val);
};

function DataStoreTestMachine (globaldatastorename) {
  this.globaldatastorename = globaldatastorename;
  this.datastore = null;
  this.communications = new DataStoreCommunications();
  this.outgoingItemsCountDefer = null;
  this.tempError = null;
}
DataStoreTestMachine.prototype.destroy = function () {
  this.tempError = null;
  if (this.outgoingItemsCountDefer) {
    this.outgoingItemsCountDefer.reject(new lib.Error('ALREADY_DYING', 'This instance of '+this.constructor.name+' is dying'));
  }
  this.outgoingItemsCountDefer = null;
  if (this.communications){
    this.communications.destroy();
  }
  this.communications = null;
  var ds = this.datastore;
  this.datastore = null;
  this.globaldatastorename = null;
  return ds ? ds.ldb.drop() : q(true);
};
DataStoreTestMachine.prototype.init = function () {
  var d = q.defer(), p = d.promise;
  //new allex_leveldbdatastorelib('test.datastore.db', fetcher, innerKeyer, d);
  new allex_leveldbdatastorelib(
    'test.datastore.db',
    this.myOuterFetcher.bind(this),
    this.outer2InnerKeyFunc.bind(this),
    d);
  return p.then(this.onInit.bind(this));
};
DataStoreTestMachine.prototype.onInit = function (ds) {
  this.datastore = ds;
  setGlobal(this.globaldatastorename || 'DataStore', ds);
  return this;
};
DataStoreTestMachine.prototype.setError = function (reason) {
  this.tempError = reason;
};
DataStoreTestMachine.prototype.expectOuterFetcherCount = function (expected) {
  if (this.outgoingItemsCountDefer) {
    this.outgoingItemsCountDefer.reject(new lib.Error('NEVER_ENCOUNTERED_AN_OUTGOING_REQUEST', 'This defer never entered the outgoing functionality from DataStore'));
  }
  this.outgoingItemsCountDefer = new AwaiterJob(expected);
  return this.outgoingItemsCountDefer.go();
};
DataStoreTestMachine.prototype.myOuterFetcher = function (keys) {
  if (this.outgoingItemsCountDefer) {
    this.outgoingItemsCountDefer.resolve(keys.length);    
  }
  return this.outerFetcherCallback(keys);
};
DataStoreTestMachine.prototype.outerFetcherCallback = function (keys){
  throw new lib.Error('NOT_IMPLEMENTED', 'outerFetcherCallback has to be implemented in descendant classes of '+this.constructor.name);
};
DataStoreTestMachine.prototype.outer2InnerKeyFunc = function (thingy) {
  throw new lib.Error('NOT_IMPLEMENTED', 'outer2InnerKeyFunc has to be implemented in descendant classes of '+this.constructor.name);
};
DataStoreTestMachine.prototype.fetchWhatever = function (thingy) {
  return DataStore.fetch(thingy).then(
    this.handleOuterResponse.bind(this)
  );
};
DataStoreTestMachine.prototype.handleOuterResponse = function (responsearray) {
  return responsearray;
};
DataStoreTestMachine.prototype.fetchNames = function (names) {
  if (!(lib.isArray(names) && names.length>0)) return;
  return this.fetchWhatever(names.map(this.nameArrayizer.bind(this)));
};
DataStoreTestMachine.prototype.arrayize = function (names){
  return (lib.isArray(names) && names.length>0) ? names.map(this.nameArrayizer.bind(this)) : [];
};
DataStoreTestMachine.prototype.nameArrayizer = function (name) {
  return {
    name: name,
    whatever: Math.floor(Math.random() * 100)
  };
};

setGlobal('DataStoreTestMachine', DataStoreTestMachine);