describe('Test with Reuters Datascope', function () {
  loadClientSide(['allex_reuters_talkerlib', 'allex_leveldbdatastorelib']);
  it ('Create a Reuters talker', function () {
    return setGlobal('ReutersTalker', new reuters_talkerlib('9011813', 'Mica1.Tatic2'));
  });
  it ('Create a DataStore', function () {
    var d = q.defer();
    new leveldbdatastorelib('test.datastore.db', ReutersTalker.extractFor.bind(ReutersTalker), d);
    return setGlobal('DataStore', d.promise);
  });
  it ('Do a request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch(['IBM.N', 'MSFT.BA', 'AAPL.BA']), 'fetch');
  });
});
