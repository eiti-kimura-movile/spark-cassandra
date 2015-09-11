CREATE KEYSPACE IF NOT EXISTS movile WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;
USE movile;

DROP TABLE idx_config_ref;
CREATE TABLE idx_config_ref (
    conf_id int,
    ref_id varchar,
    subs_key varchar,
    data varchar,
    last_charge timestamp,
    expiration timestamp,
    last_charge_att timestamp,
    last_queued timestamp,
    status_id int,
    origin_id int,
    enabled boolean,
    PRIMARY KEY (conf_id, ref_id, subs_key)
);

INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, last_queued, origin_id) VALUES (512, '1001', 'subs-key-111', '{json data= 1};', dateof(now()), 0, true, dateof(now()), dateof(now()), 1);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id,last_charge_att) VALUES (512, '1001', 'subs-key-222', '{json data 2};', dateof(now()), 1, true, dateof(now()),1,  dateof(now()));
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1001', 'subs-key-333', '{json data 3};', dateof(now()), 1, true, dateof(now()),1);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1001', 'subs-key-999', '{json data 4};', dateof(now()), 1, true, dateof(now()),1);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1001', 'subs-key-777', '{json data 5 };', dateof(now()), 0, true, dateof(now()),1);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1001', 'subs-key-778', '{json data 7 };', dateof(now()), 0, false, dateof(now()),1);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1002', 'subs-key-779', '{json data 8 };', dateof(now()), 2, false, dateof(now()),2);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1002', 'subs-key-757', '{json data 9 };', dateof(now()), 2, false, dateof(now()),2);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1002', 'subs-key-776', '{json data 0 };', dateof(now()), 2, true, null,2);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1003', 'subs-key-770', '{json data - };', dateof(now()), 0, true, null,2);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id) VALUES (500, '1003', 'subs-key-770', '{json data - };', dateof(now()), 0, true, dateof(now()),2);
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id,last_charge_att) VALUES (500, '', 'subs-key-761', '{json data 785 };', dateof(now()), 0, true, dateof(now()),2, dateof(now()));
INSERT INTO "idx_config_ref" (conf_id, ref_id, subs_key, data, last_charge, status_id, enabled, expiration, origin_id,last_charge_att) VALUES (500, '', 'subs-key-762', '{json data 852 };', dateof(now()), 0, true, dateof(now()),2, dateof(now()));
