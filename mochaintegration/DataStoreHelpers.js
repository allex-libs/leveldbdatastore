function DataStoreHelpers() {  
}
DataStoreHelpers.getAll = function (datastore) {
  var ret = [];
  return datastore.ldb.traverse(function (keyval) {ret.push(keyval);})
    .then(function () {
        var r = ret;
        ret = null;
        return r;
    });
};
DataStoreHelpers.allKeys = function (datastore) {
  var ret = [];
  return datastore.ldb.traverse(function (keyval) {ret.push(keyval.key);})
    .then(function () {
        var r = ret;
        ret = null;
        return r;
    });
};
DataStoreHelpers.itemCount = function (datastore) {
  var cnt = 0;
  return datastore.ldb.traverse(function (keyval) {cnt++;})
    .then(function () {
        var c = cnt;
        cnt = null;
        return c;
    });
};

setGlobal('DataStoreHelpers', DataStoreHelpers);