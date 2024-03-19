USE whale_sightings;

DELIMITER //
CREATE PROCEDURE insert_or_update_location(wb_name VARCHAR(255))
-- Procedure for handling logic of creating ids for location table rows based on unique name values
BEGIN
    DECLARE highest_id INT DEFAULT -1;
    DECLARE name_exists INT DEFAULT 0;

    -- Unique constraint doesn't work on NULL values.
    -- check if the name is NULL and if a NULL already exists
    IF wb_name IS NULL THEN
        SELECT 1 INTO name_exists FROM locations WHERE waterBody IS NULL LIMIT 1;
    ELSE
        -- check if waterBody name already exists
        SELECT 1 INTO name_exists FROM locations WHERE waterBody = wb_name LIMIT 1;
    END IF;

    -- if name doesn't exist, insert
    If name_exists = 0 THEN
        SELECT IFNULL(MAX(id),-1) INTO highest_id FROM locations;
        INSERT INTO locations (id, waterBody) VALUES (highest_id + 1, wb_name)
        ON DUPLICATE KEY UPDATE waterBody=wb_name;
    END IF;

    -- return the value of id that was just inserted for foreign key insert in occurrences table
    SELECT highest_id + 1 AS wb_id;
END;
//
DELIMITER ;