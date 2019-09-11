var ZeroString = String.fromCharCode(0);
function instrument2key (instrument) {
	return instrument.id+ZeroString+instrument.type;
}

function TheQuery () {
}
TheQuery.prototype.getTemplateName = function () {
	return "Composite";
};
TheQuery.prototype.getContentFieldNames = function () {
  return ["User Defined Identifier", "Instrument ID", "Universal Close Price"];
};
TheQuery.prototype.getOptions = function () {
  return {
    Condition: {
      ScalableCurrency: true
    }
  };
};

var _Query = new TheQuery();

function onExtraction (defer, results) {
  console.log('results?', require('util').inspect(results, {depth:7, colors:true}));
  defer.resolve(results[0]);
  defer = null;
}
function extractor (identifiers) {
  var d = q.defer(), ret = d.promise;
  ReutersTalker.runExtraction(identifiers, _Query).then(
    //onExtraction.bind(null, d),
    d.resolve.bind(d),
    d.reject.bind(d),
    d.notify.bind(d)
  );
  d = null;
  return ret;
}

describe('Test with Reuters Datascope', function () {
  loadClientSide(['allex:reuters_talker:lib', 'allex:leveldbdatastore:lib']);
  it ('Create a Reuters talker', function () {
    return setGlobal('ReutersTalker', new allex__reuters_talkerlib('9010985', 'Mica1.Tatic2', 'talkerlogs'));
  });
  it ('Create a DataStore', function () {
    var d = q.defer();
    //new allex_leveldbdatastorelib('test.datastore.db', ReutersTalker.extractFor.bind(ReutersTalker), instrument2key, d);
    new allex_leveldbdatastorelib('test.datastore.db', extractor, instrument2key, d);
    return setGlobal('DataStore', d.promise);
  });
  it ('Do a request', function () {
    this.timeout(100000);
    return qlib.promise2console(DataStore.fetch([{type: 'Ric', id: 'IBM.N', userid: 'IBM'}, {type: 'Ric', id: 'MSFT.BA', userid: 'M$'}, {type: 'Ric', id: 'AAPL.BA', userid: 'Apple'}]), 'fetch');
  });
});
