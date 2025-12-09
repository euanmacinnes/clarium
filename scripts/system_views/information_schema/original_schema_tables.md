-- information_schema.sql_features definition

-- Drop table

-- DROP TABLE information_schema.sql_features;

CREATE TABLE information_schema.sql_features (
	feature_id information_schema."character_data" COLLATE "C" NULL,
	feature_name information_schema."character_data" COLLATE "C" NULL,
	sub_feature_id information_schema."character_data" COLLATE "C" NULL,
	sub_feature_name information_schema."character_data" COLLATE "C" NULL,
	is_supported information_schema."yes_or_no" COLLATE "C" NULL,
	is_verified_by information_schema."character_data" COLLATE "C" NULL,
	"comments" information_schema."character_data" COLLATE "C" NULL
);


-- information_schema.sql_implementation_info definition

-- Drop table

-- DROP TABLE information_schema.sql_implementation_info;

CREATE TABLE information_schema.sql_implementation_info (
	implementation_info_id information_schema."character_data" COLLATE "C" NULL,
	implementation_info_name information_schema."character_data" COLLATE "C" NULL,
	integer_value information_schema."cardinal_number" NULL,
	character_value information_schema."character_data" COLLATE "C" NULL,
	"comments" information_schema."character_data" COLLATE "C" NULL
);


-- information_schema.sql_parts definition

-- Drop table

-- DROP TABLE information_schema.sql_parts;

CREATE TABLE information_schema.sql_parts (
	feature_id information_schema."character_data" COLLATE "C" NULL,
	feature_name information_schema."character_data" COLLATE "C" NULL,
	is_supported information_schema."yes_or_no" COLLATE "C" NULL,
	is_verified_by information_schema."character_data" COLLATE "C" NULL,
	"comments" information_schema."character_data" COLLATE "C" NULL
);


-- information_schema.sql_sizing definition

-- Drop table

-- DROP TABLE information_schema.sql_sizing;

CREATE TABLE information_schema.sql_sizing (
	sizing_id information_schema."cardinal_number" NULL,
	sizing_name information_schema."character_data" COLLATE "C" NULL,
	supported_value information_schema."cardinal_number" NULL,
	"comments" information_schema."character_data" COLLATE "C" NULL
);