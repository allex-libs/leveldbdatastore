function simpleKeyValuer (key) {
  return q([key, key+'_value']);
}

function simpleOuterFetcher (keys) {
  return q.all(keys.map(simpleKeyValuer));
}

describe('Test a simple datastore', function () {
  loadClientSide(['allex_leveldbdatastorelib']);
  it('Clear existing database', function () {
  });
  it ('Create a DataStore', function () {
    var d = q.defer();
    new allex_leveldbdatastorelib('test.datastore.db', simpleOuterFetcher, null, d);
    return setGlobal('DataStore', d.promise);
  });
  it ('Do a request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch(['a', 'b', 'c']), 'fetch');
  });
  it ('Clear DataStore', function () {
    return DataStore.ldb.drop();
  });
});
