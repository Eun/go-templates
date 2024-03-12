-- +goose Up
CREATE TABLE users (
    id                  UUID       DEFAULT UUID() PRIMARY KEY,
    name                text       NOT NULL,
    email               text       NOT NULL,
    created_at          timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          timestamp  NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) WITH SYSTEM VERSIONING
    PARTITION BY SYSTEM_TIME INTERVAL 1 HOUR AUTO PARTITIONS 48;
-- READ https://mariadb.com/kb/en/system-versioned-tables/

CREATE EVENT hourly_drop_old_users_partitions
   ON SCHEDULE
   EVERY 1 HOUR
   STARTS DATE_FORMAT(NOW(), '%Y-%m-%d 02:00:00')
DO CALL drop_old_partitions('users', DATE_SUB(NOW(), INTERVAL 1 DAY));


-- +goose Down
DROP TABLE users;
DROP EVENT hourly_drop_old_users_partitions;
