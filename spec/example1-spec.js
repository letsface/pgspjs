'use strict';

var child_process = require('child_process');
var spConfig = require('./spconfig.js').newConfig();

describe(__filename, function() {
  // console.log(JSON.stringify(spConfig));
  it('runs correctly', function(done) {
    var child = child_process.execFile(
      process.execPath, [__dirname + '/../examples/example1.js', spConfig.dsn], {},
      function(error, stdout, stderr) {
        if (error) {
          done(error);
        }
        expect(stderr.toString()).toEqual('');
        expect(stdout.toString()).toEqual(
          'connecting to ' + spConfig.dsn + '\n' + JSON.stringify({
            strtest: 'Hello world',
            step1: 0,
            step2: 10,
            step3: 20
          }) + '\n');
        done();
      }
    );
  })
});