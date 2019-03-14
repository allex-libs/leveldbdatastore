function sequence(min, max, step) {
  var ret = [], i;
  for (i=min; i<=max; i+=step) {
    ret.push(i+'');
  }
  return ret;
}

function times2 (val) {
  return parseFloat(val)*2;
}
function itemproducer (val) {
  return q([val, times2(val)]);
}
function qvalues (keys) {
  return keys.map(itemproducer);
}
function valuedFetcher (keys) {
  return q.all(qvalues(keys));
  //return q.delay(2+Math.round(Math.random()*200), values(keys));
}

describe('Test Sequentiality', function () {
  loadClientSide(['allex_leveldbdatastorelib']);
  it ('Create a DataStore', function () {
    var d = q.defer();
    new allex_leveldbdatastorelib('test.datastore.db', valuedFetcher, null, d);
    return setGlobal('DataStore', d.promise);
  });
  it ('Do odd request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch(sequence(1,10,2)), 'fetch');
  });
  it ('Do full request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch(sequence(1,10,1)), 'fetch');
  });
  it ('Clear DataStore', function () {
    return DataStore.ldb.drop();
  });
});
