create table kafka_source.accounts
(
    timestamp datetime                     not null,
    name      varchar(100) charset utf8mb3 null,
    password  varchar(30)                  null,
    email     varchar(50)                  null,
    id        int auto_increment
        primary key
);

INSERT INTO kafka_source.accounts (timestamp, name, password, email, id)
VALUES ('2025-06-17 10:00:52', 'ming', 'ming', 'zxc81906119@gmail.com', 1);
INSERT INTO kafka_source.accounts (timestamp, name, password, email, id)
VALUES ('2025-06-17 10:20:52', 'ming1', 'ming1', 'zxc81906119@gmail.com', 2);
