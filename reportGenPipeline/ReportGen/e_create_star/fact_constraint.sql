ALTER TABLE Fact1_Wash_count
ADD CONSTRAINT uq_fact1_site_yq UNIQUE (site_client_id, yq_id);
