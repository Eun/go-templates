-- +goose Up
SET GLOBAL event_scheduler=ON;

-- +goose StatementBegin
CREATE PROCEDURE drop_old_partitions(in_table_name VARCHAR(255), in_date DATE)
    LANGUAGE SQL
    NOT DETERMINISTIC
    SQL SECURITY INVOKER
BEGIN
    DECLARE partitions_to_drop VARCHAR(255);
    DECLARE sql_query VARCHAR(255);

    SET sql_query = CONCAT('DELETE HISTORY FROM ', in_table_name, ' BEFORE SYSTEM_TIME ', in_date);
    PREPARE stmt FROM sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SELECT GROUP_CONCAT(PARTITION_NAME SEPARATOR ', ') INTO partitions_to_drop
    FROM INFORMATION_SCHEMA.PARTITIONS
    WHERE
        TABLE_NAME = in_table_name AND
        TABLE_ROWS = 0 AND
        UNIX_TIMESTAMP(PARTITION_DESCRIPTION) < UNIX_TIMESTAMP(in_date) AND
        PARTITION_METHOD = 'SYSTEM_TIME';

    SET sql_query = CONCAT('ALTER TABLE ', in_table_name, ' DROP PARTITION ', partitions_to_drop);
    PREPARE stmt FROM sql_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;
-- +goose StatementEnd

-- +goose Down
DROP PROCEDURE drop_old_partitions;
