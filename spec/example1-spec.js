'use strict';

var example1 = require('../examples/example1.js');
var spconfig = require('./spconfig.js')

describe(__filename, function() {
  // console.log(JSON.stringify(spconfig));
  it('runs correctly', function(done) {
    example1
      .main(spconfig)
      .then(function(output) {
        expect(output).toEqual({ 
          strtest: 'Hello world', 
          step1: 0, 
          step2: 10, 
          step3: 20 
        });
      })
      .then(done)
      .fail(done)
      .done();
  })
});