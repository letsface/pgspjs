# Overview

Discover and auto-proxy PostgreSQL stored procedures as a dot-notation fluent interface for Node.JS with a proper transaction interface.

## Use

A typical pattern for databases is to not grant direct low-level SELECT/INSERT/UPDATE SQL operations but expose instead stored procedures in the database server.

This improves:

* security: GRANT EXECUTE on a per-role basis enforced by the DB
* code size: one location for business logic
* performance: less round-trip between database and middleware

This is particularly attractive with more common languages such as Javascript (through plv8) being available in the database.

## Examples

* See examples/example1.js