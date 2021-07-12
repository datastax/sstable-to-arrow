CREATE TABLE IF NOT EXISTS baselines.iot (
	machine_id uuid,
	sensor_name text,
	time timestamp,
	data text,
	sensor_value double,
	station_id uuid,
	PRIMARY KEY ((machine_id, sensor_name), time))
	WITH ID = 57f852b0-e066-11eb-bb5e-9116fc548b6b
	AND CLUSTERING ORDER BY (time DESC)
	AND bloom_filter_fp_chance = 0.01
	AND dclocal_read_repair_chance = 0.1
	AND crc_check_chance = 1.0
	AND default_time_to_live = 0
	AND gc_grace_seconds = 864000
	AND min_index_interval = 128
	AND max_index_interval = 2048
	AND memtable_flush_period_in_ms = 0
	AND read_repair_chance = 0.0
	AND speculative_retry = '99PERCENTILE'
	AND comment = ''
	AND caching = { 'keys': 'ALL', 'rows_per_partition': 'NONE' }
	AND compaction = { 'max_threshold': '32', 'min_threshold': '4', 'compaction_window_size': '60', 'compaction_window_unit': 'MINUTES', 'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' }
	AND compression = { 'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor' }
	AND cdc = false
	AND extensions = {  };
