CREATE OR REPLACE FUNCTION example1_test_function()
RETURNS text
AS
$$
DECLARE
    strresult text;
BEGIN
	SELECT 'Hello world' INTO strresult;
	RETURN strresult;
END;
$$
LANGUAGE plpgsql;


DROP TABLE IF EXISTS example1_table ;
CREATE TABLE example1_table(record_name NAME, record_value int);

INSERT INTO example1_table VALUES('testrecord', 42);

CREATE OR REPLACE FUNCTION example1_modify(record_name NAME, record_value int)
RETURNS void
AS
$$
BEGIN
	EXECUTE format('UPDATE example1_table SET record_value = $1 WHERE record_name = $2')
		USING record_value, record_name;
END;
$$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION example1_retrieve(record_name TEXT)
RETURNS int
AS
$$
DECLARE
    record_value int;
BEGIN
 EXECUTE format('SELECT record_value FROM example1_table WHERE record_name = $1')
 	USING record_name INTO record_value;

 RETURN record_value;
END;
$$
LANGUAGE plpgsql;
