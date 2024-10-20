CREATE DATABASE info21
  WITH OWNER = postgres
  ENCODING = 'UTF8'
  CONNECTION LIMIT = -1
  IS_TEMPLATE = False;

\c info21

SET datestyle TO "ISO, YMD";

CREATE TABLE Peers (
	Nickname character varying(50) PRIMARY KEY CHECK(Nickname <> ''),
	Birthday date NOT NULL CHECK(Birthday >= '1923-01-01' AND Birthday < current_date - '17 YEAR'::interval)
);

CREATE TABLE Tasks (
	Title varchar(100) Primary KEY,
	ParentTask varchar(100) REFERENCES TASKS(Title),
	MaxXP integer NOT NULL -- проверка на хп >= 0?
);

--ENUM for checks
CREATE TYPE CheckStatus AS ENUM('Start', 'Success', 'Failure');

CREATE TABLE Checks (
	ID bigint PRIMARY KEY,
	Peer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	Task varchar(100) REFERENCES Tasks(Title) ON UPDATE CASCADE,
  "Date" date NOT NULL
);

CREATE TABLE P2P(
	ID bigint PRIMARY KEY,
	"Check" bigint REFERENCES Checks(ID) ON UPDATE CASCADE,
	CheckingPeer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	State CheckStatus CHECK(State IN ('Start', 'Success', 'Failure')),
	"Time" time NOT NULL,
  UNIQUE("Check", CheckingPeer, State)
);

CREATE TABLE Verter (
	ID bigint PRIMARY KEY,
	"Check" bigint REFERENCES Checks(ID) ON UPDATE CASCADE,
	State CheckStatus CHECK(state IN ('Start', 'Success', 'Failure')),
	"Time" time NOT NULL,
  UNIQUE ("Check", State)
 );

CREATE TABLE TransferredPoints (
	ID bigint PRIMARY KEY,
	CheckingPeer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	CheckedPeer  varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	PointsAmount integer NOT NULL,
	UNIQUE (CheckingPeer, CheckedPeer),
	CHECK (CheckingPeer <> CheckedPeer),
  CHECK (PointsAmount > 0)
);

CREATE TABLE Friends (
	ID bigint PRIMARY KEY,
	Peer1 character varying(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE, --ON DELETE CASCADE? (надо обсудить)
	Peer2 character varying(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	UNIQUE (Peer1, Peer2),
	CHECK (Peer1 <> Peer2)
);

CREATE TABLE Recommendations (
	ID bigint PRIMARY KEY,
	Peer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	RecommendedPeer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	UNIQUE (Peer, RecommendedPeer),
	CHECK (Peer <> RecommendedPeer)
);

CREATE TABLE XP (
	ID bigint PRIMARY KEY,
	"Check" bigint REFERENCES CHECKS(ID) ON UPDATE CASCADE,
	XPAmount integer NOT NULL,
  UNIQUE(ID, "Check")
);

CREATE TABLE TimeTracking (
	ID bigint PRIMARY KEY,
	Peer varchar(50) REFERENCES Peers(Nickname) ON UPDATE CASCADE,
	"Date" date NOT NULL,
	"Time" time NOT NULL,
	State integer NOT NULL CHECK(State = 1 OR State = 2)
);
--триггер чтобы не допускать комбинаций (peer1, peer2) (peer2, peer1), так как по условию они взаимные друзья 
CREATE OR REPLACE FUNCTION fnc_trg_friends_check() RETURNS TRIGGER AS $$
	BEGIN
    NEW.Peer1 = lower(NEW.Peer1);
    NEW.Peer2 = lower(NEW.Peer2);
		IF TG_OP = 'INSERT'
			AND ((NEW.Peer2, NEW.Peer1) NOT IN (SELECT Peer1, Peer2 FROM Friends)) AND NEW.Peer1 IS NOT NULL
      AND NEW.Peer2 IS NOT NULL AND NEW.peer1 <> NEW.peer2 THEN
        NEW.ID := (SELECT COALESCE(MAX(ID) + 1, 1) FROM Friends);
	  		RETURN NEW;
    ELSIF TG_OP = 'UPDATE' 
      AND (NEW.Peer2, NEW.Peer1) NOT IN (SELECT Peer1, Peer2 FROM Friends WHERE ID <> OLD.ID) THEN
        RETURN NEW;
		END IF;
		RETURN NULL;
	END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER trg_friends_check
	BEFORE INSERT OR UPDATE ON Friends
	FOR EACH ROW
	EXECUTE PROCEDURE fnc_trg_friends_check();

--перевод ник пира в нижний регистр
CREATE OR REPLACE FUNCTION fnc_trg_peers() RETURNS TRIGGER AS $$
	BEGIN
    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
      NEW.Nickname = lower(NEW.Nickname);
      RETURN NEW;      
    END IF;
		RETURN NULL;
	END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER trg_peers
	BEFORE INSERT OR UPDATE ON Peers
	FOR EACH ROW
	EXECUTE PROCEDURE fnc_trg_peers();

--Procedure for import and export data from/to file
CREATE OR REPLACE PROCEDURE import_table(name varchar, sour varchar, delim char = ',') AS $$
  BEGIN
      EXECUTE format ('COPY %s FROM %L DELIMITERS %L CSV', name, sour, delim);
	END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE export_table(name varchar, sour varchar, delim char = ',') AS $$
  BEGIN
      EXECUTE format ('COPY %s TO   %L DELIMITERS %L CSV', name, sour, delim);
	END;
$$ LANGUAGE PLPGSQL;

--import table
CALL import_table('peers', '/Users/kareemto/SQL2_Info21_v1.0-1/src/peers.csv', ',');
CALL import_table('tasks', '/Users/kareemto/SQL2_Info21_v1.0-1/src/tasks.csv', ',');
CALL import_table('checks', '/Users/kareemto/SQL2_Info21_v1.0-1/src/checks.csv', ',');
CALL import_table('p2p', '/Users/kareemto/SQL2_Info21_v1.0-1/src/p2p.csv', ',');
CALL import_table('verter', '/Users/kareemto/SQL2_Info21_v1.0-1/src/verter.csv', ',');
CALL import_table('transferredpoints', '/Users/kareemto/SQL2_Info21_v1.0-1/src/transferredpoints.csv', ',');
CALL import_table('friends', '/Users/kareemto/SQL2_Info21_v1.0-1/src/friends.csv', ',');
CALL import_table('recommendations', '/Users/kareemto/SQL2_Info21_v1.0-1/src/recommendations.csv', ',');
CALL import_table('xp', '/Users/kareemto/SQL2_Info21_v1.0-1/src/xp.csv', ',');
CALL import_table('timetracking', '/Users/kareemto/SQL2_Info21_v1.0-1/src/timetracking.csv', ',');
--export example
CALL export_table('peers', '/Users/kareemto/SQL2_Info21_v1.0-1/src/test-peers.txt', '*');
CALL export_table('xp', '/Users/kareemto/SQL2_Info21_v1.0-1/src/test-xp.txt', '+');
