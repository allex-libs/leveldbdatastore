var JobBase = qlib.JobBase;

function DataStoreCommunication (datastore, data, options, defer) {
  JobBase.call(this, defer);
  this.datastore = datastore;
  this.data = data;
  this.block = null;
  this.initFromOptions(options);
}
lib.inherit(DataStoreCommunication, JobBase);
DataStoreCommunication.prototype.destroy = function () {
  if (this.block) {
    this.block.reject(new lib.Error('ALREADY_DESTROYED', 'DataStoreCommunication dying'));
  }
  this.block = null;
  this.datastore = null;
  JobBase.prototype.destroy.call(this);
};
DataStoreCommunication.prototype.initFromOptions = function (options){
  if (!options) return;
  if (options.block) {
    this.block = q.defer();
    return;
  }
};
DataStoreCommunication.prototype.go = function () {
  var ok = this.okToGo();
  if (!ok.ok) {
    return ok.val;
  }
  if (this.block) {
    qlib.promise2defer(
      this.block.promise.then(this.sendingProc.bind(this)),
      this
    );
    return ok.val;
  }
  qlib.promise2defer(
    this.sendingProc(),
    this
  );
  return ok.val;
};
DataStoreCommunication.prototype.sendingProc = function () {
  if (!this.okToProceed()){
    return;
  }
  return this.datastore.fetch(this.data);
};
DataStoreCommunication.prototype.unblock = function (reason) {
  if (!this.block) return;
  if (lib.isVal(reason) && this.block) {
    this.block.reject(reason);
    return;
  }
  this.block.resolve(true);
};

setGlobal('DataStoreCommunication', DataStoreCommunication);


function DataStoreCommunications () {
  this.jobMap = new lib.Map();
  this.promiseMap = new lib.Map();
}
DataStoreCommunications.prototype.destroy = function () {
  if (this.jobMap) {
    lib.containerDestroyAll(this.jobMap);
    this.jobMap.destroy();
  }
  if (this.promiseMap) {
    this.promiseMap.destroy();
  }
  this.promiseMap = null;
  this.jobMap = null;
};
DataStoreCommunications.prototype.add = function (name, datastore, data, options) {
  var ret = new DataStoreCommunication(datastore, data, options);
  this.jobMap.add(name, ret);
};
DataStoreCommunications.prototype.run = function (name) {
  if (lib.isVal(this.promiseMap.get(name))) return;
  this.promiseMap.add(name, this.jobMap.get(name).go());
};
DataStoreCommunications.prototype.addRunning = function (name, datastore, data, options) {
  this.add(name, datastore, data, options);
  this.run(name);
};
DataStoreCommunications.prototype.get = function (name) {
  return this.jobMap.get(name);
};
DataStoreCommunications.prototype.getPromise = function (name) {
  return this.promiseMap.get(name);
};
DataStoreCommunications.prototype.getPromises = function (names) {
  return names.map(this.getPromise.bind(this));
};
DataStoreCommunications.prototype.unblock = function (name, arg) {
  this.jobMap.get(name).unblock(arg);
};
DataStoreCommunications.prototype.clear = function (name) {
  this.promiseMap.remove(name);
  this.jobMap.remove(name);
};

setGlobal('DataStoreCommunications', DataStoreCommunications);
