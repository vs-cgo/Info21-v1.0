--1)
CREATE OR REPLACE FUNCTION transferredpoints() RETURNS TABLE ("Peer1" varchar, "Peer2" varchar, "PointsAmount" integer) AS $$
	BEGIN
		RETURN QUERY
			SELECT t.CheckingPeer AS "Peer1", t.CheckedPeer AS "Peer2",
			COALESCE(t.PointsAmount, 0) - COALESCE(t2.PointsAmount, 0) AS "PointsAmount" FROM TransferredPoints t 
			LEFT JOIN TransferredPoints t2 ON t.CheckingPeer = t2.CheckedPeer AND t.CheckedPeer = t2.CheckingPeer
			WHERE t2.ID IS NULL OR t.ID < t2.ID ORDER BY t.ID;
	END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM transferredpoints();

--2)
CREATE OR REPLACE FUNCTION get_grade() RETURNS TABLE ("Peer" varchar, "Task" varchar, "XP" integer) AS $$
  DECLARE
  res record;
  BEGIN
    FOR res IN (SELECT * FROM Checks ORDER BY ID) LOOP
      IF EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = res.ID AND p.state = 'Success') AND
			(SELECT COUNT(State) FROM Verter v WHERE v."Check" = res.ID AND (v.state = 'Success' OR v.state = 'Start')) <> 1 THEN
        "Peer" = res.Peer;
        "XP" = (SELECT XPAmount FROM XP WHERE XP."Check" = res.ID ORDER BY ID LIMIT 1);
        "Task" = split_part(res.Task, '_', 1);
        RETURN NEXT;
      END IF;
    END LOOP;
    RETURN;
  END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM get_grade();

--3)
CREATE OR REPLACE FUNCTION not_leave(value date) RETURNS TABLE ("Peer" varchar) AS $$ 
  BEGIN
    RETURN QUERY 
      SELECT Peer AS "Peer" FROM TimeTracking t WHERE "Date" = $1 GROUP BY Peer HAVING COUNT(state) = 2;  
    END;
$$ LANGUAGE PLPGSQL;

SELECT * FROM not_leave('2023-09-27');

--4)
 CREATE OR REPLACE PROCEDURE change_prp(cur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN cur FOR
    WITH tmp AS (
      SELECT CheckingPeer AS p1, SUM(PointsAmount)::integer AS s1 FROM TransferredPoints GROUP BY CheckingPeer  
			UNION ALL
      SELECT CheckedPeer AS p1, SUM(PointsAmount)::integer * -1 AS s1 FROM TransferredPoints GROUP BY CheckedPeer
    )
    SELECT Nickname AS "Peer", COALESCE(SUM(s1), 0) AS "PointsChange" FROM Peers p 
			LEFT JOIN tmp ON tmp.p1 = p.Nickname GROUP BY 1 ORDER BY 2 DESC; 
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL change_prp();
FETCh ALL IN refcur;
COMMIT;

--5)
CREATE OR REPLACE PROCEDURE change_prp2(cur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN cur FOR 
    WITH tmp AS (
      SELECT "Peer1" AS Peer, SUM("PointsAmount") AS sumpoint FROM transferredpoints() GROUP BY "Peer1"
			UNION ALL
      SELECT "Peer2" as Peer, SUM("PointsAmount") * -1 AS sumpoint FROM transferredpoints() GROUP BY "Peer2"
    )
    SELECT Nickname AS "Peer", COALESCE(SUM(sumpoint), 0) AS "PointsChange"
    FROM Peers p LEFT JOIN tmp ON p.Nickname = tmp.Peer GROUP BY 1 ORDER BY 2 DESC;
    END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL change_prp2();
FETCH ALL IN refcur;
COMMIT;

--6)
CREATE OR REPLACE PROCEDURE check_task(refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN refcur FOR 
    WITH tmp AS (
      SELECT "Date" As Day, Task, COUNT(Task) AS amount From Checks 
      INNER JOIN P2P p ON Checks.ID = p."Check" AND p.State = 'Start' GROUP BY "Date", Task
    ) 
    SELECT to_char(Day, 'dd.mm.yyyy') AS "Day", split_part(Task, '_', 1) AS "Task" FROM tmp t1 
      WHERE amount = (SELECT MAX(amount) FROM tmp t2 WHERE t1.Day = t2.Day) ORDER BY Day DESC;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL check_task();
FETCh ALL IN refcur;
COMMIT;

--7)
DROP PROCEDURE IF EXISTS find_peers(varchar, refcursor); 
CREATE OR REPLACE PROCEDURE find_peers(name varchar, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN refcur FOR
    WITH tmp1 AS (
      SELECT Title, length(split_part(Title, '_', 1)) AS len FROM Tasks t
        WHERE substring(split_part(Title, '_', 1) from '(([-A-z]{1,}\s*){1,})') = name
    ), tmp2 AS (
      SELECT Peer, Task, MAX("Date") AS Day, COUNT(*) OVER(PARTITION BY Peer) as amount, (SELECT COUNT(*) FROM tmp1) AS amount_task
        FROM Checks c INNER JOIN tmp1 t ON c.Task = t.Title 
          AND EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = c.ID AND p.State = 'Success')
          AND (SELECT COUNT(*) FROM Verter v WHERE v."Check" = c.ID AND (v.State = 'Start' OR v.State = 'Success')) <> 1
        GROUP BY Peer, Task
    )
    SELECT Peer AS "Peer", to_char(Day, 'dd.mm.yyyy') AS "Day" FROM tmp2 
    WHERE amount = amount_task AND Task = (SELECT Title From tmp1 ORDER BY len DESC, title DESC LIMIT 1) ORDER BY Day DESC;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL find_peers('C');
FETCH ALL IN refcur;
CLOSE refcur;
CALL find_peers('DO');
FETCH ALL IN refcur;
CLOSE refcur;
CALL find_peers('SQL');
FETCH ALL IN refcur;
COMMIT;

--8)
CREATE OR REPLACE PROCEDURE find_peer_to_check(refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  OPEN refcur FOR
  WITH tmp AS (
    SELECT Peer1, Peer2 FROM Friends
    UNION
    SELECT Peer2, Peer1 FROM Friends
    ORDER BY 1, 2
  ), tmp2 AS (
    SELECT Peer1 As Peer, RecommendedPeer, COUNT(*) AS c FROM tmp t 
    INNER JOIN Recommendations r ON t.Peer2 = r.Peer AND t.Peer1 <> r.RecommendedPeer 
    GROUP BY Peer1, RecommendedPeer 
  )
  SELECT Peer AS "Peer", RecommendedPeer AS "RecommendedPeer"
    FROM tmp2 WHERE c = (SELECT MAX(c) FROM tmp2 t2 WHERE tmp2.Peer = t2.Peer) ORDER BY Peer;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL find_peer_to_check();
FETCH ALL IN refcur;
COMMIT;

--9)
CREATE OR REPLACE PROCEDURE percent_peer(block1 varchar, block2 varchar, refcur refcursor DEFAULT 'refcur') AS $$
  DECLARE 
    all_peer integer := (SELECT COUNT(*) FROM Peers);
  BEGIN
	IF all_peer <> 0 THEN 
		OPEN refcur FOR
		WITH tmp AS (
			SELECT Nickname AS Peer, substring(split_part(Task, '_', 1) from '(([-A-z]{1,}\s*){1,})') AS block FROM Checks c 
				RIGHT JOIN Peers ON Nickname = c.Peer GROUP BY 1, 2
		), tmp2 AS (
			SELECT Peer, Count(*) FILTER (WHERE block = block1) AS S1, Count(*) FILTER (WHERE block = block2) AS S2
			FROM tmp GROUP BY Peer
		)
		SELECT ROUND(COUNT(*) FILTER (WHERE S1 = 1 AND S2 = 0) * 100.0 / all_peer) AS "StartedBlock1",
					 ROUND(COUNT(*) FILTER (WHERE S1 = 0 AND S2 = 1) * 100.0 / all_peer) AS "StartedBlock2",
					 ROUND(COUNT(*) FILTER (WHERE S1 = 1 AND S2 = 1) * 100.0 / all_peer) AS "StartedBothBlocks", 
					 ROUND(COUNT(*) FILTER (WHERE S1 = 0 AND S2 = 0) * 100.0 / all_peer) AS "DidntStartAnyBlock"
		FROM tmp2;
	ELSE 
		RAISE NOTICE 'Таблица Peers пуста';
	END IF;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL percent_peer('DO', 'SQL');
FETCH ALL IN refcur;
COMMIT;

--10)
CREATE OR REPLACE PROCEDURE percent_birthday(refcur refcursor DEFAULT 'refcur') AS $$
  DECLARE
		all_peer integer;
	BEGIN
    OPEN refcur FOR 
    WITH tmp AS (
      SELECT c.ID, c.Peer FROM Checks c
        INNER JOIN Peers p ON EXTRACT(DAY FROM c."Date") = EXTRACT(DAY FROM p.Birthday)
        AND EXTRACT(MONTH FROM c."Date") = EXTRACT(MONTH FROM p.Birthday) AND c.Peer = p.Nickname
        GROUP BY ID
    ), tmp2 AS (
      SELECT Peer, CASE 
        WHEN EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = tmp.ID AND p.State = 'Success') AND 
             (SELECT COUNT(State) FROM Verter v WHERE v."Check" = tmp.ID AND v.State IN ('Start', 'Success')) <> 1  THEN 1
        WHEN EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = tmp.ID AND p.State = 'Failure') OR 
             EXISTS (SELECT 1 FROM Verter v WHERE v."Check" = tmp.ID AND v.State = 'Failure') THEN 0
        ELSE -1 END AS Success FROM tmp GROUP BY 1, 2
    )
    SELECT ROUND((COUNT(DISTINCT Peer) FILTER (WHERE success = 1)) * 100.0
        /CASE WHEN COUNT(DISTINCT Peer) = 0 THEN 1 ELSE COUNT(DISTINCT Peer) END) AS "SuccessfulChecks",
      ROUND((COUNT(DISTINCT Peer) FILTER (WHERE success = 0)) * 100.0
          /CASE WHEN COUNT(DISTINCT Peer) = 0 THEN 1 ELSE COUNT(DISTINCT Peer) END) AS "UnsuccessfulChecks" 
    FROM tmp2;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL percent_birthday();
FETCH ALL IN refcur;
COMMIT;

--11) 
CREATE OR REPLACE PROCEDURE find_peers3(task1 varchar, task2 varchar, task3 varchar, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN refcur FOR 
    WITH tmp AS ( 
    SELECT c.Peer, Task FROM Checks c INNER JOIN P2P p ON c.ID = p."Check" AND Task IN (task1, task2, task3) 
			AND p.State = 'Success' 
			AND (SELECT COUNT(State) FROM Verter v WHERE c.ID = v."Check" AND v.State IN ('Start','Success')) <> 1
    GROUP BY c.Peer, Task
    ), tmp2 AS (
      SELECT Peer, COUNT(*) FILTER (WHERE Task = task1 OR Task = task2) AS c12, COUNT(*) FILTER (WHERE Task = task3) AS c3  
      FROM tmp GROUP BY Peer
    )
    SELECT Peer AS "Peer" FROM tmp2 WHERE c12 = 2 AND c3 = 0;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL find_peers3('C2_SimpleBashUtils', 'DO2_Linux_Network', 'SQL2_Info21_v1.0');
FETCH ALL IN refcur;
COMMIT;

--12)
CREATE OR REPLACE PROCEDURE amount_steps(refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
    OPEN refcur FOR 
    WITH RECURSIVE rec AS (
    SELECT Title, 0 AS step FROM Tasks WHERE ParentTask IS NULL
    UNION 
    SELECT t.Title, step + 1 From rec r INNER JOIN Tasks t ON r.Title = t.ParentTask
    )
    SELECT Title AS "Task", step AS "PrevCount" FROM rec;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL amount_steps();
FETCH ALL IN refcur;
COMMIT;

--13)
CREATE OR REPLACE PROCEDURE find_lucky(N integer, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  IF N > 0 THEN 
    OPEN refcur FOR 
    WITH RECURSIVE rec AS (
      SELECT "Date", "Time", CASE WHEN State <= 0 OR Percent < 80 THEN 0 ELSE 1 END AS r, 1 AS i FROM (
        SELECT c."Date", p."Time", CASE
          WHEN EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = c.ID AND p.State = 'Success')
            AND (SELECT COUNT(State) FROM Verter v WHERE v."Check" = c.ID AND v.State IN ('Start', 'Success')) <> 1 THEN 1
          WHEN EXISTS (SELECT 1 FROM P2P p WHERE p."Check" = c.ID AND p.State = 'Failure')
            OR EXISTS (SELECT 1 From Verter v WHERE v."Check" = c.ID AND v.State = 'Failure') THEN 0
          ELSE -1 END AS State, COALESCE(XPAmount, 0) * 100.0 / MaxXP AS Percent, 1 AS step
        FROM Checks c INNER JOIN P2P p ON c.ID = p."Check" AND p.State = 'Start'
        LEFT JOIN XP on c.ID = XP."Check"
        LEFT JOIN Tasks t ON c.Task = t.Title
        ORDER BY 1, 2, 3) tmp
      UNION
      SELECT "Date", "Time", CASE WHEN r <= 0 THEN r 
        WHEN LAG(r, 1, 0) OVER (PARTITION BY "Date" ORDER BY "Date", "Time", r) >= i THEN r+1 ELSE r END AS r, i+1 FROM rec
        WHERE i < N
    )
    SELECT "Date" AS "Day" FROM rec WHERE i = N AND r >= N GROUP BY "Date";
  ELSE 
    RAISE NOTICE 'Запрашиваемый поиск kоличества подряд идущих успешных проверок должен быть больше 0';
    OPEN refcur FOR SELECT NULL AS "Day";
  END IF;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL find_lucky(6);
FETCH ALL IN refcur;
COMMIT;

--14)
CREATE OR REPLACE PROCEDURE maximum_xp(refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  OPEN refcur FOR 
    WITH tmp AS (
      SELECT Peer, Task, COALESCE(MAX(XPAmount), 0) AS XP
      FROM Checks c INNER JOIN XP ON c.ID = XP."Check" 
      GROUP BY Peer, Task ORDER BY Peer, Task
    ), tmp2 AS (
      SELECT Peer, SUM(XP) AS XP FROM tmp GROUP BY Peer
    )
    SELECT Peer AS "Peer", XP AS "XP" FROM tmp2 WHERE XP = (SELECT MAX(XP) FROM tmp2); 
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL maximum_xp();
FETCH ALL IN refcur;
COMMIT;

--15) 
CREATE OR REPLACE PROCEDURE came_in(t time, N integer, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  OPEN refcur FOR 
    WITH tmp AS (
      SELECT Peer, COUNT(DISTINCT "Date") AS freq FROM TimeTracking WHERE "Time" < t AND State = 1 GROUP BY Peer 
    )
    SELECT Peer AS "Peer" FROM tmp WHERE freq >= N;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL came_in('12:00:00', 30);
FETCH ALL IN refcur;
COMMIT;

--16) 
CREATE OR REPLACE PROCEDURE came_out(N integer, M integer, refcur refcursor DEFAULT 'refcur') AS $$
  BEGIN
  OPEN refcur FOR 
    WITH tmp AS (
       SELECT Peer, COUNT(*) - COUNT(DISTINCT "Date") AS freq
       FROM TimeTracking WHERE "Date" >= CURRENT_DATE - N AND "Date" <= CURRENT_DATE AND State = 2 GROUP BY Peer
    )
    SELECT Peer AS "Peer" FROM tmp WHERE freq > M;
  END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL came_out(27, 6);
FETCH ALL IN refcur;
COMMIT;

--17) 
CREATE OR REPLACE PROCEDURE early_entry(refcur refcursor DEFAULT 'refcur') AS $$
	BEGIN
	OPEN refcur FOR 
	WITH tmp AS (
		SELECT t.Peer, "Date", MIN("Time") AS enter, TO_CHAR("Date", 'Month') AS m FROM TimeTracking t 
			INNER JOIN Peers p ON t.Peer = p.Nickname AND TO_CHAR(Birthday, 'Month') = TO_CHAR("Date", 'Month')
			WHERE State = 1 GROUP BY Peer, "Date", 4
	), tmp2 AS (
		SELECT TO_CHAR(i, 'Month') as mon, enter, i
		FROM generate_series('2023-01-01'::date, '2023-12-01', '1 Month') AS g(i)
		LEFT JOIN tmp ON m = TO_CHAR(i, 'Month')
	)
	SELECT mon AS "Mounth", ROUND(COUNT(mon) FILTER (WHERE enter < '12:00:00') * 100.0/ 
													CASE WHEN COUNT(mon) = 0 THEN 1 ELSE COUNT(mon) END, 0) AS "EarlyEntries" 
	FROM tmp2 GROUP BY mon, i ORDER BY i;
	END;
$$ LANGUAGE PLPGSQL;

BEGIN;
CALL early_entry();
FETCH ALL IN refcur;
COMMIT;
