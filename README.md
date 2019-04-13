![Authrus Database](https://github.com/authrus/service/raw/master/gateway/authrus-gateway/src/main/resources/static/logo.png)

Event source database that supports a catalog of tables and supports a variant of ANSI SQL. This database is built upon events, such
that all state is stored in a linear event log. Inserts, updates, and deletions are all appended to this event log. When the database
starts it simple reads from the origin of the log and plays the events back in to the internal catalog.

A key feature here is the ability to run multiple instances of the database and remotely connect them. The separate instances
will subscribe to each others event log and apply mutations to its state machine representing the catalog of tables. Consistency is
eventual, however the replication performance for co-located instances is in the micro-second time frame.

#### Features

* Supports a ANSI SQL, including transactions and alter statements
* Web based SQL terminal console 
* State is stored and recovered from an event log
* Real-time state replication
* Schema generation from Java types
