# RedisJ
A basic Java port of Redis server protocol.

This projects aims at implementing the server side of Redis in plain Java 
without any further dependencies mainly for testing purposes, e.g. unit tests.

Although not all commands are implemented, the most common data types strings and lists
are supported including expiration/TTL. The server also supports persisting data to
local disk, however this is still very rudimentary and not well tested.

I am trying to keep everythin in a single file such that there is no need to add 
dependencies to a project but just dropping this file in your test source folder
should be sufficient.
 
