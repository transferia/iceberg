DROP TABLE IF EXISTS `cdc_test`;
CREATE TABLE `cdc_test` (
    `id`   BIGINT PRIMARY KEY,
    `name` VARCHAR(256) NOT NULL,
    `val`  INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `cdc_test` (id, name, val) VALUES
    (1, 'alice', 100),
    (2, 'bob', 200),
    (3, 'charlie', 300);
