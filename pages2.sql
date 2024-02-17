CREATE TABLE pages2 (
    `_id` INT AUTO_INCREMENT NOT NULL,
    `url` VARCHAR(768) NOT NULL,
    `status` SMALLINT NOT NULL,
    `encoding` VARCHAR(16) NOT NULL,
    `headers` VARCHAR(1024) NOT NULL,
    `date` TIMESTAMP,
    `body` MEDIUMTEXT NOT NULL,
    PRIMARY KEY (`_id`),
    UNIQUE KEY `url_key` (`url`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;