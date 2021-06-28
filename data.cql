CREATE KEYSPACE IF NOT EXISTS hello WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': '1' };

CREATE TABLE IF NOT EXISTS hello.foo (
    asciival ascii PRIMARY KEY,
    bigintval bigint,
    blobval blob,
    booleanval boolean,
--    counterval counter,
    dateval date,
    decimalval decimal,
    doubleval double,
    durationval duration,
    floatval float,
--    frozenval frozen,
    inetval inet,
    intval int,
--    listval list,
--    mapval map,
--    setval set,
    smallintval smallint,
    textval text,
    timeval time,
    timestampval timestamp,
    timeuuidval timeuuid,
    tinyintval tinyint,
--    tupleval tuple,
    uuidval uuid,
    varcharval varchar,
    varintval varint
) WITH compression = { 'enabled': 'false' };

insert into hello.foo (
    asciival , bigintval , blobval , booleanval , dateval , decimalval , doubleval , durationval , floatval , inetval , intval , smallintval , textval , timestampval , timeuuidval , timeval , tinyintval , uuidval , varcharval , varintval
) VALUES (
    'foo', 2048, intAsBlob(4096), false, '2021-06-25', 2.718281828, 3.14159265, 12h30m50ms, 6.28, '127.0.0.1', 420, 50, 'barbarbarbar', '2021-06-25T23:40:05.718Z', now(), '23:39:52' , 24, 4e944ad2-d60f-11eb-b8bc-0242ac130003, 'varcharvalvarcharval', 7801234567
);