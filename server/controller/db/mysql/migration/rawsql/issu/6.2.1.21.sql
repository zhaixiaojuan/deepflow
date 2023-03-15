ALTER TABLE resource_group_extra_info ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;

UPDATE db_version SET version = '6.2.1.21';
