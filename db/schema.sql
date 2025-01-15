
CREATE TABLE sources (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  source_name TEXT NOT NULL,
  source_type TEXT NOT NULL,
  source_description TEXT NOT NULL,
  config BLOB NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE destinations (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  destination_name TEXT NOT NULL,
  destination_type TEXT NOT NULL,
  destination_description TEXT NOT NULL,
  config BLOB NOT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE pipelines (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  source_id BIGINT NOT NULL,
  destination_id BIGINT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES sources(id),
  FOREIGN KEY (destination_id) REFERENCES destinations(id)
);
