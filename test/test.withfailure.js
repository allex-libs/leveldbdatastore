var testlib = require('./lib');

function createMyMachine () {
  function Machine () {
    DataStoreTestComplexMachine.call(this);
  }
  lib.inherit(Machine, DataStoreTestComplexMachine);
  Machine.prototype.destroy = function () {
    DataStoreTestComplexMachine.prototype.destroy.call(this);
  };
  Machine.prototype.ensureFirstSuccessDoesNotOverlap = function () {
    return this.fetchNames(this.firstRequestNames).then(
      this.onFirstRefetched.bind(this)
    );
  };
  Machine.prototype.onFirstRefetched = function (refetched) {
    var a = refetched;
  };
  Machine.prototype.firstRequestNames = ['a', 'b', 'c', 'a', 'b', 'a'];
  Machine.prototype.secondRequestNames = ['c', 'd', 'e', 'd'];
  Machine.prototype.outerFetcherCallback = function (keys) {
    return DataStoreTestComplexMachine.prototype.outerFetcherCallback.call(this, keys);
  };
  Machine.prototype.handleOuterResponse = function (responsearray) {
    return DataStoreTestComplexMachine.prototype.handleOuterResponse.call(this, responsearray);
  };
  
  setGlobal('Machine', Machine);
}

function firstTwoWaiterFunc () {
  this.timeout(1e4);
  //return machine.waitForFirstTwo();
  return  q.allSettled(machine.communications.getPromises(['first', 'second'])).then(
    function (sett){
      sett.should.be.an('array').with.length(2);
      sett[0].state.should.equal('rejected');
      sett[1].state.should.equal('fulfilled');
      return q.all([
        DataStoreHelpers.itemCount(machine.datastore).then(cnt => {
          cnt.should.equal(3);
        }),
        DataStoreHelpers.allKeys(machine.datastore).then(keys => {
          keys.should.be.an('array').that.includes.members(['c', 'd', 'e']);
          keys.length.should.equal(3);
        }),
      ]);
    }
  )
}
function secondTwoWaiterFunc () {
  this.timeout(1e4);
  //return machine.waitForFirstTwo();
  return  q.allSettled(machine.communications.getPromises(['first', 'second'])).then(
    function (sett){
      sett.should.be.an('array').with.length(2);
      sett[0].state.should.equal('fulfilled');
      sett[1].state.should.equal('fulfilled');
      return q.all([
        DataStoreHelpers.itemCount(DataStore).then(cnt => {
          cnt.should.equal(5);
        }),
        DataStoreHelpers.allKeys(DataStore).then(keys => {
          keys.should.be.an('array').that.includes.members(['a', 'b', 'c', 'd', 'e']);
          keys.length.should.equal(5);
        }),
      ]);
    }
  )
}

describe('Test a complex datastore', function () {
  loadMochaIntegration('allex_leveldbdatastorelib');
  loadClientSide(['allex:leveldbdatastore:lib']);
  it('Create test Classes', createMyMachine);
  it('Create a Machine', function () {
    setGlobal('machine', new Machine());
  });
  it('Init machine', function () { return machine.init(); });
  it('Send first request (that will fail)', function () { 
    machine.communications.addRunning(
      'first',
      DataStore,
      machine.arrayize(machine.firstRequestNames),
      {block: true}    );
   });
   it('Send second, overlapping, request (that will succeed)', function () { 
    machine.communications.addRunning(
      'second',
      DataStore,
      machine.arrayize(machine.secondRequestNames),
      {block: true}    );
   } );
  it('Abrupt first', function () {
    machine.communications.unblock('first', new lib.Error('INTENTIONAL_ABRUPTION', 'Intentional Abruption'));
  });
  it('Resolve second', function () {
    var p = machine.expectOuterFetcherCount(3);
    machine.communications.unblock('second');
    return p;
  });
  it('Wait for first two requests to settle', firstTwoWaiterFunc);
  it('Send first request again (that will succeed)', function () { 
    machine.communications.clear('first');
    machine.communications.addRunning(
      'first',
      DataStore,
      machine.arrayize(machine.firstRequestNames),
      {block: true}    );
   });
   it('Resolve first', function () {
    machine.communications.unblock('first');
  });
  it('Wait for first two requests to settle again', secondTwoWaiterFunc);
  it ('Make sure that succeeding first response does not overlap the second', 
    function () {
      return machine.ensureFirstSuccessDoesNotOverlap();
    });
  it('Destroy machine', function () { return machine.destroy(); });
});
