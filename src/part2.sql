-- 1)
CREATE OR REPLACE PROCEDURE add_p2p(peer1 varchar, peer2 varchar, task_name varchar, status CheckStatus, "time" time) AS $$
  DECLARE
    tmp bigint;
		check_end int;
		check_start int;
  BEGIN
    peer1 := lower(peer1);
    peer2 := lower(peer2);
    IF (EXISTS (SELECT 1 FROM Peers WHERE Nickname = peer1) AND EXISTS (SELECT 1 FROM Peers WHERE Nickname = peer2)) THEN 
		  SELECT COUNT(p.ID) FROM P2P p INNER JOIN CHECKS c ON p."Check" = c.ID 
			  WHERE c.Peer = peer1 AND c.Task = task_name AND p.State = 'Start' INTO check_start;
		  SELECT COUNT(p.ID) FROM P2P p INNER JOIN CHECKS c ON p."Check" = c.ID 
			  WHERE c.Peer = peer1 AND c.Task = task_name AND (p.State = 'Success' OR p.State = 'Failure') INTO check_end;
      
      IF status = 'Start'  AND check_start - check_end = 0 THEN
			  INSERT INTO Checks VALUES ((SELECT COALESCE(MAX(ID) + 1, 1) FROM Checks), peer1, task_name, CURRENT_DATE) RETURNING ID INTO tmp;
			  INSERT INTO P2P VALUES ((SELECT COALESCE(MAX(ID) + 1, 1) FROM P2P), tmp, peer2, status, "time");
      ELSIF (status = 'Success' OR status = 'Failure') AND check_start - check_end = 1 THEN
        SELECT c.ID INTO tmp FROM Checks c INNER JOIN P2P p ON c.ID = p."Check" 
          WHERE c.Peer = peer1 AND c.Task = task_name AND p.CheckingPeer = peer2 
          GROUP BY 1 HAVING COUNT(State) FILTER (WHERE State = 'Start') = 1 
						AND COUNT(State) FILTER (WHERE State IN ('Success', 'Failure')) = 0;  
        IF tmp IS NOT NULL THEN
          INSERT INTO P2P VALUES((SELECT COALESCE(MAX(ID) + 1, 1) FROM P2P), tmp, peer2, status, "time");
        ELSE
					RAISE NOTICE '% has started check this task with another peer or incorret data', peer1;
        END IF;
      END IF;
    ELSE
      RAISE NOTICE 'Relationship Peers dont have one or both(%, %) nickname', peer1, peer2;
    END IF;
  END;
$$ LANGUAGE PLPGSQL;

-- 2)
CREATE OR REPLACE PROCEDURE add_verter (peer_name varchar, task_name varchar, status CheckStatus,"time" time) AS $$
  DECLARE 
    tmp bigint;
		check_end integer;
		check_start integer;
  BEGIN
    peer_name = lower(peer_name);
    IF EXISTS (SELECT 1 FROM Peers WHERE Nickname = peer_name) THEN
			IF status = 'Start' THEN
				SELECT c.ID FROM Checks c INNER JOIN P2P p ON p."Check" = c.ID WHERE c.Peer = peer_name AND c.Task = task_name
					AND p.state = 'Success' AND (SELECT COUNT(State) FROM Verter v WHERE v."Check" = c.ID) = 0
					ORDER BY c."Date" DESC, p."Time" DESC LIMIT 1 INTO tmp;
				IF tmp IS NOT NULL THEN
					INSERT INTO Verter VALUES((SELECT COALESCE(MAX(ID) + 1, 1) FROM Verter), tmp, status, "time");
				ELSE 
					RAISE NOTICE 'У % нет удачной завершенной Р2Р проверки или некорректные данные', peer_name;
				END IF;
		  ELSIF (status = 'Success' OR status = 'Failure') THEN
				SELECT c.ID FROM Checks c INNER JOIN P2P p ON p."Check" = c.ID WHERE c.Peer = peer_name AND c.Task = task_name
					AND p.state = 'Success' AND (SELECT COUNT(State) FILTER (WHERE State = 'Start') FROM Verter v WHERE v."Check" = c.ID) = 1 
					AND (SELECT COUNT(State) FILTER (WHERE State IN ('Success', 'Failure')) FROM Verter v WHERE v."Check" = c.ID) = 0
					ORDER BY c."Date" DESC, p."Time" DESC LIMIT 1 INTO tmp;
				IF tmp IS NOT NULL THEN
			    INSERT INTO Verter VALUES(COALESCE((SELECT MAX(ID) + 1 FROM Verter), 1), tmp, status, "time");
        ELSE 
					RAISE NOTICE 'У % нет удачной завершенной Р2Р проверки или некорректные данные', peer_name;
				END IF;
			END IF;
		ELSE 
      RAISE NOTICE 'Relationship Peers dont have nickname %', peer_name;
    END IF;
	END;
$$ LANGUAGE PLPGSQL;

-- 3)
CREATE OR REPLACE FUNCTION fnc_trg_transferredpoints() RETURNS TRIGGER AS $$
	DECLARE 
    p varchar(50);
    i integer;
  BEGIN
		IF NEW.state = 'Start' THEN      
      SELECT Peer FROM Checks c WHERE c.ID = NEW."Check" INTO p;
      IF p IS NOT NULL THEN
        SELECT PointsAmount FROM TransferredPoints WHERE CheckingPeer = NEW.CheckingPeer AND CheckedPeer = p INTO i;
        IF i IS NOT NULL THEN
			    UPDATE TransferredPoints SET PointsAmount = i + 1 
			    WHERE CheckingPeer = NEW.CheckingPeer AND CheckedPeer = p;
        ELSE 
          INSERT INTO TransferredPoints 
          VALUES((SELECT COALESCE(MAX(ID) + 1, 1) FROM TransferredPoints), NEW.CheckingPeer, p, 1);
        END IF;
      END IF;
		END IF;
		RETURN NEW;
	END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER trg_transferredpoints
	AFTER INSERT ON P2P
	FOR EACH ROW 
	EXECUTE PROCEDURE fnc_trg_transferredpoints();

-- 4)
CREATE OR REPLACE FUNCTION fnc_trg_xp() RETURNS TRIGGER AS $$
	BEGIN
		IF EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = NEW."Check" AND State = 'Success') 
      AND (EXISTS (SELECT 1 FROM Verter v WHERE v."Check" = NEW."Check" AND State = 'Success') 
				OR (SELECT COUNT(State) FROM Verter v WHERE v."Check" = NEW."Check") = 0)
      AND NEW.XPAmount <= (SELECT MaxXP FROM Checks c INNER JOIN Tasks t ON c.ID = NEW."Check" AND c.Task = t.Title) THEN
			NEW.ID = COALESCE((SELECT MAX(ID) FROM XP) + 1,1);
			RETURN NEW;
		END IF;
	RETURN NULL;
	END;
$$ LANGUAGE PLPGSQL;

CREATE TRIGGER trg_xp 
	BEFORE INSERT ON XP
	FOR EACH ROW
	EXECUTE PROCEDURE fnc_trg_xp();

INSERT INTO Peers VALUES('peer_13', '1979-09-17');

--for task 1
CALL add_p2p ('peer_4', 'peer_8', 'SQL2_Info21_v1.0', 'Start', '12:30:00');
CALL add_p2p ('peer_13', 'peer_12', 'C2_SimpleBashUtils', 'Start', '12:30:00');
CALL add_p2p ('peer_13', 'peer_12', 'C2_SimpleBashUtils', 'Success', '12:57:17');
--for task 2
CALL add_verter ('peer_13', 'C2_SimpleBashUtils', 'Start', '12:58:02');
CALL add_verter ('peer_13', 'C2_SimpleBashUtils', 'Success', '13:04:01');
--for task 3
SELECT 'peer_12`s points after check peer_13' as "INFO", PointsAmount FROM TransferredPoints 
  WHERE CheckingPeer = 'peer_12' AND CheckedPeer = 'peer_13'; 
--for task 4
INSERT INTO XP VALUES (1, 159, 600);--неудачная(нет успеха)
INSERT INTO XP VALUES (1, 160, 360);--неудачная(xp  больше чем может быть)
INSERT INTO XP VALUES (1, 160, 340);--удачная
--2 проверка для peer_13 на 350 xp
CALL add_p2p ('peer_13', 'peer_11', 'C2_SimpleBashUtils', 'Start', '16:30:00');
CALL add_p2p ('peer_13', 'peer_11', 'C2_SimpleBashUtils', 'Success', '16:55:41');
CALL add_verter ('peer_13', 'C2_SimpleBashUtils', 'Start', '16:56:11');
CALL add_verter ('peer_13', 'C2_SimpleBashUtils', 'Success', '16:58:59');
INSERT INTO XP VALUES (1, 161, 350);
