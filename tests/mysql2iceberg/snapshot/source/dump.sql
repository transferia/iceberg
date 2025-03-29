DROP TABLE IF EXISTS `mysql_snapshot`;
CREATE TABLE `mysql_snapshot` (
    `i` INT AUTO_INCREMENT PRIMARY KEY,
    `ti` TINYINT,
    `si` SMALLINT,
    `mi` MEDIUMINT,
    `bi` BIGINT,

    `f` FLOAT,
    `dp` DOUBLE PRECISION,

    `b` BOOL,

    `c10` CHAR(10),
    `vc20` VARCHAR(20),
    `tx` TEXT,

    `d` DATE,
    `t` TIME,
    `dt` DATETIME,
    `ts` TIMESTAMP,
    `y` YEAR
) engine=innodb default charset=utf8;

INSERT INTO `mysql_snapshot` (ti, si, mi, bi, f, dp,  b, c10, vc20, tx, d, t, dt, ts, y) VALUES
(
    0, -- ti
    0, -- si
    0, -- mi
    0, -- bi

    0.0, -- f
    0.0, -- dp

    false, -- b

    '          ', -- c10
    '                    ', -- c20
    '', -- tx
    '1970-01-01', -- d
    '00:00:00.000000', -- t
    '1900-01-01 01:00:00.000000', -- dt
    '1971-01-01 01:00:00.000000', -- ts
    '1901' -- y
),
(
    127, -- ti
    32767, -- si
    8388607, -- mi
    9223372036854775807, -- bi

    1.1, -- f
    1.1, -- dp

    true, -- b

    'char1char1', -- c10
    'char1char1char1char1', -- c20
    'text-text-text', -- tx
    '1999-12-31', -- d
    '01:02:03.456789', -- t
    '1999-12-31 23:59:59.999999', -- dt
    '1999-12-31 23:59:59.999999', -- ts
    '1999' -- y
),
(
    -128, -- ti
    -32768, -- si
    -8388608, -- mi
    -9223372036854775808, -- bi

    1.1, -- f
    1.1, -- dp

    true, -- b

    'sant" '' CL', -- c10
    'sant" '' CLAWS \\\\\\\\""', -- c20
    'ho-ho-ho my name is "Santa" ''CLAWS\\', -- tx
    '2038-12-31', -- d
    '23:59:59.999999', -- t
    '2106-02-07 06:28:15.999999', -- dt
    '2037-01-19 04:14:06.999999', -- ts
    '2155' -- y
);
