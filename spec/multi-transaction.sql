CREATE OR REPLACE FUNCTION create_entity(identifier TEXT, doc json)
RETURNS json
AS
$$
BEGIN
	return doc;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION update_entity(identifier TEXT, doc json)
RETURNS json
AS
$$
BEGIN
	return doc;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION query_entity(identifier TEXT, doc json)
RETURNS json
AS
$$
BEGIN
	return doc;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION remove_entity(identifier TEXT, doc json)
RETURNS json
AS
$$
BEGIN
	return doc;
END;
$$
LANGUAGE plpgsql;