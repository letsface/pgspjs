# Overview

Discover and auto-proxy PostgreSQL stored procedures as a dot-notation fluent interface for Node.JS with a proper transaction interface.

A typical pattern for databases is to not grant direct low-level SELECT/INSERT/UPDATE SQL operations but to grant execute permissions instead on stored procedures in the database server.

This improves:

* security: GRANT EXECUTE on a per-role basis enforced by the DB
* code size: one location for business logic
* performance: less round-trip between database and middleware

This is particularly attractive with more common languages such as Javascript (through plv8) being available in the database.

## Dependencies

* PostgreSQL database
* fluentchain (https://github.com/letsface/fluentchain)

## Usage

* npm install git+ssh://git@github.com/letsface/pgspjs.git
* var pgspjs = require('pgspjs');
* pgspjs.exportStoredProcs(dataSourceName) 
 * returns a Q promise to an object with a .transaction()
* Call transaction() and use dot notation with your stored procedures names

## Examples

* See examples/example1.js