CREATE DATABASE IF NOT EXISTS whale_sightings;
use whale_sightings;


CREATE TABLE `locations` (
  `id` int NOT NULL,
  `waterBody` varchar(255) UNIQUE NULL,
  PRIMARY KEY (`id`)
);


CREATE TABLE `species` (
  `id` int NOT NULL,
  `speciesName` varchar(50) NOT NULL,
  `vernacularName` varchar(50) NOT NULL,
  PRIMARY KEY (`id`)
);


CREATE TABLE `occurrences` (
  `id` varchar(150) NOT NULL,
  `eventDate` varchar(50) NOT NULL,
  `waterBodyId` int DEFAULT NULL,
  `latitude` decimal(9,7) NOT NULL,
  `longitude` decimal(10,7) NOT NULL,
  `speciesId` int DEFAULT NULL,
  `individualCount` int NOT NULL,
  `start_year` int NOT NULL,
  `start_month` int NOT NULL,
  `start_day` int NOT NULL,
  `end_year` int NOT NULL,
  `end_month` int NOT NULL,
  `end_day` int NOT NULL,
  `date_is_valid` BOOLEAN NOT NULL,
  PRIMARY KEY (`id`),
  CONSTRAINT `fk_waterBodyId`
  FOREIGN KEY (`waterBodyId`)
  REFERENCES `locations`(`id`)
  ON UPDATE CASCADE,
  CONSTRAINT `fk_speciesId`
  FOREIGN KEY (`speciesId`)
  REFERENCES `species`(`id`)
  ON UPDATE CASCADE
);
