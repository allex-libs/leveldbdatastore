function complexProducer (results) {
  console.log('results', results);
  return q(['complex', results]);
}

function complexKeyValuer1 (key) {
  return q([key, {first: key.name+'_value1'}]);
}

function complexKeyValuer2 (key) {
  return q([key, {second: key.name+'_value2'}]);
}

function concater (values) {
  return q(values[0].concat(values[1]));
}

function fetchProducer (keys) {
  return q.all([
    q.all(keys.map(complexKeyValuer1)),
    q.all(keys.map(complexKeyValuer2))
  ]).then(concater);
}

function complexOuterFetcher (keys) {
  return fetchProducer(keys).then(
    complexProducer
  );
}

function innerKeyer (val) {
  return val.name;
}

describe('Test a complex datastore', function () {
  loadClientSide(['allex:leveldbdatastore:lib']);
  it ('Create a DataStore', function () {
    var d = q.defer();
    new allex_leveldbdatastorelib('test.datastore.db', complexOuterFetcher, innerKeyer, d);
    return setGlobal('DataStore', d.promise);
  });
  it ('Do the request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch([{name: 'a', call: 1}, {name: 'b', call: 1}, {name: 'c', call: 1}]), 'fetch');
  });
  it ('Clear DataStore', function () {
    return DataStore.ldb.drop();
  });
});
