CREATE KEYSPACE IF NOT EXISTS testtypes1 WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

CREATE TABLE IF NOT EXISTS testtypes1.ascii (id int PRIMARY KEY, asciival ascii) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.ascii (id, asciival) VALUES (1, 'this is an ascii string');

CREATE TABLE IF NOT EXISTS testtypes1.bigint (id int PRIMARY KEY, bigintval bigint) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.bigint (id, bigintval) VALUES (1, 1234567);

CREATE TABLE IF NOT EXISTS testtypes1.blob (id int PRIMARY KEY, blobval blob) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.blob (id, blobval) VALUES (1, intAsBlob(1234));

CREATE TABLE IF NOT EXISTS testtypes1.boolean (id int PRIMARY KEY, booleanval boolean) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.boolean (id, booleanval) VALUES (1, true);

-- CREATE TABLE IF NOT EXISTS testtypes1.counter (id int PRIMARY KEY, counterval counter) WITH compression = {'enabled': 'false'};
-- INSERT INTO testtypes1.counter (id, counterval) VALUES (1, );

CREATE TABLE IF NOT EXISTS testtypes1.date (id int PRIMARY KEY, dateval date) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.date (id, dateval) VALUES (1, '2021-06-25');

CREATE TABLE IF NOT EXISTS testtypes1.decimal (id int PRIMARY KEY, decimalval decimal) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.decimal (id, decimalval) VALUES (1, 2.71828);

CREATE TABLE IF NOT EXISTS testtypes1.double (id int PRIMARY KEY, doubleval double) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.double (id, doubleval) VALUES (1, 3.14159);

CREATE TABLE IF NOT EXISTS testtypes1.duration (id int PRIMARY KEY, durationval duration) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.duration (id, durationval) VALUES (1, 12h30m50ms);

CREATE TABLE IF NOT EXISTS testtypes1.float (id int PRIMARY KEY, floatval float) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.float (id, floatval) VALUES (1, 6.28);

CREATE TABLE IF NOT EXISTS testtypes1.inet (id int PRIMARY KEY, inetval inet) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.inet (id, inetval) VALUES (1, '127.0.0.1');

CREATE TABLE IF NOT EXISTS testtypes1.int (id int PRIMARY KEY, intval int) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.int (id, intval) VALUES (1, 420);

CREATE TABLE IF NOT EXISTS testtypes1.smallint (id int PRIMARY KEY, smallintval smallint) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.smallint (id, smallintval) VALUES (1, 123);

CREATE TABLE IF NOT EXISTS testtypes1.text (id int PRIMARY KEY, textval text) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.text (id, textval) VALUES (1, 'this is a text string');

CREATE TABLE IF NOT EXISTS testtypes1.time (id int PRIMARY KEY, timeval time) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.time (id, timeval) VALUES (1, '02:46:30');

CREATE TABLE IF NOT EXISTS testtypes1.timestamp (id int PRIMARY KEY, timestampval timestamp) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.timestamp (id, timestampval) VALUES (1, '2021-07-02T20:47:23Z');

CREATE TABLE IF NOT EXISTS testtypes1.timeuuid (id int PRIMARY KEY, timeuuidval timeuuid) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.timeuuid (id, timeuuidval) VALUES (1, now());

CREATE TABLE IF NOT EXISTS testtypes1.tinyint (id int PRIMARY KEY, tinyintval tinyint) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.tinyint (id, tinyintval) VALUES (1, 24);

CREATE TABLE IF NOT EXISTS testtypes1.uuid (id int PRIMARY KEY, uuidval uuid) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.uuid (id, uuidval) VALUES (1, 0ce82e94-0aa4-497a-83c2-d8f0e0687f12);

CREATE TABLE IF NOT EXISTS testtypes1.varchar (id int PRIMARY KEY, varcharval varchar) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.varchar (id, varcharval) VALUES (1, 'this is a varchar string');

CREATE TABLE IF NOT EXISTS testtypes1.varint (id int PRIMARY KEY, varintval varint) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.varint (id, varintval) VALUES (1, 54321);

CREATE TABLE IF NOT EXISTS testtypes1.map (id int PRIMARY KEY, mapintintval map<int,int>) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.map (id, mapintintval) VALUES (1, { 42: 13 });

CREATE TABLE IF NOT EXISTS testtypes1.set_table (id int PRIMARY KEY, setintval set<int>) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.set_table (id, setintval) VALUES (1, { 2914, 4, 17 });

CREATE TABLE IF NOT EXISTS testtypes1.list (id int PRIMARY KEY, listintval list<int>) WITH compression = {'enabled': 'false'};
INSERT INTO testtypes1.list (id, listintval) VALUES (1, [ 192, 168, 1, 1 ]);
