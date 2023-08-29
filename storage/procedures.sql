DELIMITER //
CREATE PROCEDURE insert_or_update_location(wb_name VARCHAR(255))
BEGIN
    DECLARE highest_id INT DEFAULT -1;
    DECLARE name_exists INT DEFAULT 0;
    DECLARE local_wb_id INT DEFAULT 0;

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

    SELECT id INTO local_wb_id FROM locations WHERE waterBody = wb_name;

    SELECT local_wb_id AS wb_id;
END;
//

CREATE PROCEDURE insert_or_update_species(s_id INT, s_name VARCHAR(50), v_name VARCHAR(50))
BEGIN
    DECLARE local_species_id INT DEFAULT 0;

    INSERT INTO species(id, speciesName, vernacularName) VALUES(s_id, s_name, v_name)
    ON DUPLICATE KEY UPDATE speciesName=s_name, vernacularName=v_name;

    SELECT s_id INTO local_species_id;

    SELECT local_species_id AS speciesId;
END;
//
DELIMITER ;