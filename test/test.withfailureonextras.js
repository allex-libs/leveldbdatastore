var testlib = require('./lib');

function createMyMachine () {
  function Machine () {
    DataStoreTestComplexMachine.call(this);
  }
  lib.inherit(Machine, DataStoreTestComplexMachine);
  Machine.prototype.destroy = function () {
    return DataStoreTestComplexMachine.prototype.destroy.call(this);
  };
  Machine.prototype.firstRequestNames = ['a', 'b'];
  Machine.prototype.secondRequestNames = ['a', 'b', 'c', 'd', 'e'];
  
  setGlobal('Machine', Machine);
}

function firstWaiterFunc () {
  this.timeout(1e4);
  //return machine.waitForFirstTwo();
  return  q.allSettled(machine.communications.getPromises(['first'])).then(
    function (sett){
      sett.should.be.an('array').with.length(1);
      sett[0].state.should.equal('fulfilled');
      return q.all([
        DataStoreHelpers.itemCount(machine.datastore).then(cnt => {
          cnt.should.equal(2);
        }),
        DataStoreHelpers.allKeys(machine.datastore).then(keys => {
          keys.should.be.an('array').that.includes.members(['a', 'b']);
          keys.length.should.equal(2);
        }),
      ]);
    }
  )
}
function secondWaiterFunc () {
  this.timeout(1e7);
  return  q.allSettled(machine.communications.getPromises(['second'])).then(
    function (sett){
      sett.should.be.an('array').with.length(1);
      sett[0].state.should.equal('rejected');
      return q.all([
        DataStoreHelpers.itemCount(DataStore).then(cnt => {
          cnt.should.equal(2);
        }),
        DataStoreHelpers.allKeys(DataStore).then(keys => {
          keys.should.be.an('array').that.includes.members(['a', 'b']);
          keys.length.should.equal(2);
        }),
      ]);
    }
  )
}
function thirdWaiterFunc () {
  this.timeout(1e7);
  return  q.allSettled(machine.communications.getPromises(['second'])).then(
    function (sett){
      sett.should.be.an('array').with.length(1);
      sett[0].state.should.equal('fulfilled');
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

describe('Test a complex datastore with failure on extras', function () {
  loadMochaIntegration('allex_leveldbdatastorelib');
  loadClientSide(['allex:leveldbdatastore:lib']);
  it('Create test Classes', createMyMachine);
  it('Create a Machine', function () {
    return setGlobal('machine', new Machine());
  });
  it('Init machine', function () { return machine.init(); });
  it('Destroy machine (for clear init)', function () { this.timeout(1e7); return machine.destroy(); });
  it('Create a Machine (this time for real)', function () {
    return setGlobal('machine', new Machine());
  });
  it('Init machine', function () { return machine.init(); });
  it('Send first request (that will succeed)', function () { 
    machine.communications.addRunning(
      'first',
      machine,
      machine.arrayize(machine.firstRequestNames),
      {block: true}    );
   });
  it('Resolve first', function () {
    this.timeout(1e7);
    var p = machine.expectOuterFetcherCount(2);
    machine.communications.unblock('first');
    return p;
  });
  it('Wait for first request to settle', firstWaiterFunc);
  it('Send second, independent, request (that will fail)', function () { 
    machine.communications.clear('first');
    machine.communications.addRunning(
      'second',
      machine,
      machine.arrayize(machine.secondRequestNames),
      {block: true}    );
  } );
  it('Abrupt second', function () {
    machine.communications.unblock('second', new lib.Error('INTENTIONAL_ABRUPTION', 'Intentional Abruption'));
  });
  it('Wait for the second request to fail', secondWaiterFunc);
  it('Send second request again (that will succeed)', function () { 
    machine.communications.clear('second');
    machine.communications.addRunning(
      'second',
      machine,
      machine.arrayize(machine.secondRequestNames),
      {block: true}    );
   });
   it('Resolve second', function () {
    machine.communications.unblock('second');
  });
  it('Wait for the second request to succeed', thirdWaiterFunc);
  it('Destroy machine', function () { this.timeout(1e7); return machine.destroy(); });
});
