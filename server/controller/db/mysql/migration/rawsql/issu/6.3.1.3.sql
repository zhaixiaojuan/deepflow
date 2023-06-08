START TRANSACTION;

CREATE TABLE IF NOT EXISTS ch_prometheus_target_label_layout (
    `target_id`      INT(10) NOT NULL PRIMARY KEY,
    `target_labels`  TEXT
)ENGINE=innodb DEFAULT CHARSET=utf8;

-- update db_version to latest, remeber update DB_VERSION_EXPECT in migrate/init.go
UPDATE db_version SET version='6.3.1.3';
-- modify end

COMMIT;
