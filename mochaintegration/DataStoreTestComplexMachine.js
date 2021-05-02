
function DataStoreTestComplexMachine (globaldatastorename) {
  DataStoreTestMachine.call(this, globaldatastorename);
}
lib.inherit(DataStoreTestComplexMachine, DataStoreTestMachine);
DataStoreTestComplexMachine.prototype.destroy = function () {
  DataStoreTestMachine.prototype.destroy.call(this);
}
DataStoreTestComplexMachine.prototype.outerFetcherCallback = function (keys){
  return q.all([
    q.all(keys.map(this.complexKeyValuer1.bind(this))),
    q.all(keys.map(this.complexKeyValuer2.bind(this)))
  ]).then(this.complexValuesConcater.bind(this));
};
DataStoreTestComplexMachine.prototype.complexKeyValuer1 = function (key) {
  return q([key, {first: key.name+'_value1'}]);
};
DataStoreTestComplexMachine.prototype.complexKeyValuer2 = function (key) {
  return q([key, {first: key.name+'_value2'}]);
};
DataStoreTestComplexMachine.prototype.complexValuesConcater = function(values) {
  return q(values[0].concat(values[1]));
}
DataStoreTestComplexMachine.prototype.outer2InnerKeyFunc = function (thingy) {
  return thingy.name;
};
DataStoreTestComplexMachine.prototype.handleOuterResponse = function (responsearray) {
  return q(['complex', responsearray]);
};

setGlobal('DataStoreTestComplexMachine', DataStoreTestComplexMachine);