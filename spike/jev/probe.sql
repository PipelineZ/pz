INSTALL jev FROM community;
LOAD jev;
select function_name, function_type, parameters, return_type, stability from duckdb_functions() where function_name like 'jev%';
select name, description, value from duckdb_settings() where name like 'jev%';
