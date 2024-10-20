\c postgres
DROP DATABASE part4;

CREATE DATABASE part4
WITH OWNER = postgres
ENCODING = 'UTF8'
CONNECTION LIMIT = -1
IS_TEMPLATE = False;

\c part4
SET datestyle TO "ISO, YMD";

--CREATE 3 table with name (test, test2, not_test)
CREATE TABLE test (
	name character varying(20) PRIMARY  KEY,
	age integer NOT NULL
);
CREATE TABLE test2 (
	name character varying(20) PRIMARY  KEY,
	age integer NOT NULL
);
CREATE TABLE not_test (
	id integer PRIMARY KEY,
	test character varying(50)
);
CREATE TABLE not_test2 (
	id integer PRIMARY KEY,
	test character varying(50)
);

--Insert value in the tables
INSERT INTO test (VALUES ('test1', 18), ('test2', 25), ('test3', 32)); 
INSERT INTO test2 (VALUES ('test2-1', 19), ('test2-2', 27), ('test2-3', 33)); 
INSERT INTO not_test (SELECT i, 'name' || i FROM generate_series(1, 20) g(i));
INSERT INTO not_test2 (SELECT i, 'name' || i FROM generate_series(1, 10) g(i));

--1)
CREATE OR REPLACE PROCEDURE drop_table(name varchar) AS $$
	DECLARE
		tmp varchar;
	BEGIN
		name := name || '%';
		FOR tmp IN (SELECT table_name FROM information_schema.tables 
				WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
				AND table_schema IN('public', 'myschema') AND table_name SIMILAR TO name) LOOP
			EXECUTE format('DROP TABLE %I', tmp);
		END LOOP;
	END;
$$ LANGUAGE PLPGSQL;

--test for task 1
\d
CALL drop_table('test');
\d

--prosedure and function for task 2
CREATE OR REPLACE PROCEDURE s(IN a integer  = 0, IN b integer = 0, INOUT c integer = 0) AS $$
    SELECT * FROM (VALUES(b + a))t;    
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION p(IN a integer  = 0, IN b integer = 0) RETURNS integer AS $$
  BEGIN
    RETURN a * b;
  END;
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION no_par() RETURNS void AS $$
  BEGIN
    RAISE NOTICE 'Primer function without parametrs';
  END;
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION t1(INOUT x integer = 0) RETURNS integer AS $$
  SELECT x + 5;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t2(x integer = 0) RETURNS TABLE (s integer, p integer) AS $$
  SELECT i + i AS s , i * i FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t3() RETURNS TABLE (s integer, p integer) AS $$
  SELECT MAX(i + i) AS s, MAX(i * i) AS p FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t4() RETURNS record AS $$
  SELECT MAX(i + i) AS s, MAX(i * i) AS p FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t5(OUT s int, OUT p int) RETURNS SETOF record AS $$
  SELECT i + i, i * i FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t6() RETURNS  integer[] AS $$
  SELECT array_agg(i + i) FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t7(OUT s integer[], OUT p integer[]) RETURNS record AS $$
  SELECT array_agg(i + i), array_agg(i * i) FROM generate_series(1,10) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t8(VARIADIC s numeric[]) RETURNS numeric AS $$
  SELECT SUM(s[i]) FROM generate_subscripts(s, 1) g(i);
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t9(integer, integer) RETURNS integer AS $$
  SELECT $1 + $2;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t10(not_test, IN x integer = 0) RETURNS SETOF record AS $$
  SELECT *, x FROM not_test;
$$ LANGUAGE SQL;
CREATE OR REPLACE FUNCTION t11(IN name varchar, OUT len integer) RETURNS integer AS $$
  SELECT length(name);
$$ LANGUAGE SQL;

--2)
CREATE OR REPLACE PROCEDURE func(INOUT amount integer DEFAULT 0, refcur refcursor DEFAULT 'refcur') AS $$
	DECLARE 
  res record;
  BEGIN
		OPEN refcur SCROLL FOR 
		WITH tmp AS (
    SELECT proname, pg_get_function_arguments(p.oid) AS parameters
    FROM pg_proc p 
      LEFT JOIN pg_namespace n ON p.pronamespace = n.oid
      LEFT JOIN pg_language l ON p.prolang = l.oid 
      WHERE nspname <> 'pg_catalog' AND nspname <> 'information_schema' 
        AND (pronargs > 0 OR proallargtypes IS NOT NULL) AND lanname = 'sql' AND proretset = 'f'
    )
    SELECT proname AS name, parameters FROM tmp WHERE parameters IS NOT NULL AND parameters <> '';
    amount := 0;
    LOOP 
      FETCH refcur INTO res;
      EXIT WHEN NOT FOUND;
      amount := amount + 1;
    END LOOP;
    MOVE ABSOLUTE 0 IN refcur;
  END;
$$ LANGUAGE PLPGSQL;

--test for task 2
BEGIN;
CALL func();
FETCH ALL IN refcur;
COMMIT;

--trigger with trigger`s function for task 3
CREATE OR REPLACE FUNCTION del() RETURNS TRIGGER AS $$
  BEGIN
    IF TG_OP = 'INSERT' THEN
      NEW.id := COALESCE((SELECT MAX(id) + 1 FROM not_test), 1);
      NEW.test := NEW.test || '_trigger'; 
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE PLPGSQL;
CREATE OR REPLACE FUNCTION del2() RETURNS TRIGGER AS $$
  BEGIN
    IF TG_OP = 'INSERT' THEN
      NEW.id := COALESCE((SELECT MAX(id) + 1 FROM not_test2), 1);
      NEW.test := NEW.test || '_trigger'; 
    END IF;
    RETURN NEW;
  END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER for_delete
  BEFORE INSERT OR UPDATE ON not_test
  FOR EACH ROW
  EXECUTE PROCEDURE del();

CREATE TRIGGER for_delete2
  BEFORE DELETE ON not_test
  FOR EACH ROW
  EXECUTE PROCEDURE del();

CREATE TRIGGER for_delete
  BEFORE INSERT ON not_test2
  FOR EACH ROW
  EXECUTE PROCEDURE del2();
  
INSERT INTO not_test VALUES (1, 'name'), (1, 'test');
INSERT INTO not_test2 VALUES (1, 'name'), (1, 'test');

--3)
CREATE OR REPLACE PROCEDURE delete_trigger(INOUT amount integer = 0) AS $$ 
  DECLARE
    tmp record;
  BEGIN
    amount := 0;
    FOR tmp IN (SELECT tgname, relname FROM pg_trigger t INNER JOIN pg_class c ON t.tgrelid = c.oid 
        WHERE tgconstraint = 0 AND tgconstrrelid = 0 AND tgconstrindid = 0) LOOP
      EXIT WHEN tmp IS NULL;
      EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I RESTRICT', tmp.tgname, tmp.relname);
        amount := amount + 1;
    END LOOP;
  END;
$$ LANGUAGE PLPGSQL;

--variant 2
CREATE OR REPLACE PROCEDURE del_trigger_procedure(OUT coun int) AS $$
DECLARE
	rigger_name varchar;
	ble_name varchar;
BEGIN
	SELECT COUNT(*) INTO coun 
    FROM (SELECT DISTINCT trigger_name, event_object_table FROM information_schema.triggers)tmp;

	FOR rigger_name, ble_name IN (SELECT trigger_name, event_object_table
                                  FROM information_schema.triggers)
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I ', rigger_name, ble_name);
	END LOOP;
END;
$$ LANGUAGE plpgsql;

CALL delete_trigger();
--CALL del_trigger_procedure(NULL);

--4) Создать хранимую процедуру с входным параметром, которая выводит имена и описания типа объектов
CREATE OR REPLACE PROCEDURE find_str(in str varchar, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  OPEN refcur FOR 
  WITH tmp AS (
    SELECT p.proname AS name, 
      CASE WHEN prokind = 'p' THEN 'procedure' WHEN prokind = 'f' THEN 'function' ELSE 'dont be here' END AS "type",
      prosrc 
    FROM pg_proc p INNER JOIN pg_language l ON p.prolang = l.oid 
        WHERE lanname = 'sql' AND proretset = 'f'
  )
  SELECT name, "type" FROM tmp WHERE prosrc LIKE '%' || $1 || '%'; 
  END;
$$ LANGUAGE PLPGSQL;

--test for task 4
BEGIN;
CALL find_str('SELECT * FROM');
FETCH ALL IN refcur;
CLOSE refcur;
CALL find_str('SELECT MAX');
FETCH ALL IN refcur;
CLOSE refcur;
CALL find_str('generate_series');
FETCH ALL IN refcur;
COMMIT;
