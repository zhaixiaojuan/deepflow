DROP PROCEDURE IF EXISTS check_data_sources;

CREATE PROCEDURE check_data_sources()
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_source
        WHERE name NOT IN ('1s', '1m', 'flow_log.l4_flow_log', 'flow_log.l7_flow_log', 'flow_log.l4_packet', 'flow_log.l7_packet', 'deepflow_system')
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'invalid name for data_source to upgrade';
    END IF;

    UPDATE db_version SET version='6.3.1.23';
END;

CALL check_data_sources();
