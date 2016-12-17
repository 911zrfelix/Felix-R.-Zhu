--                      Grid-ased Crowed Sourced Intelligent Traffic Information Hub
--                                               Rui Zhu
--                                             rzhu@kth.se
--                             Department of Urban Planning and Environment
--                             Royal Institute of Technology - KTH, Sweden

--==================================================================================================================--
-- Part-0. The rest SQL is an optional method to covert input time to postgresql supported timestamp
--==================================================================================================================--
-- 1. create a new table
CREATE TABLE test1
(
 oid INTEGER,
 cur_t VARCHAR(14),
 x DOUBLE PRECISION,
 y DOUBLE PRECISION,
 cur_spd DOUBLE PRECISION
);

-- 2. copy .csv file into the database (use PSQL Console)
\COPY test1 FROM 'C:\Program Files\PostgreSQL\8.4\data\csv\test1.csv' WITH DELIMITER ',' CSV HEADER

CREATE INDEX oid_index ON test1(oid);
CREATE INDEX cur_t_index ON test1(cur_t);

-- 3. add columns
ALTER TABLE test1 ADD COLUMN ct TIMESTAMP;
ALTER TABLE test1 ADD COLUMN yy VARCHAR(4);
ALTER TABLE test1 ADD COLUMN mm VARCHAR(2);
ALTER TABLE test1 ADD COLUMN dd VARCHAR(2);
ALTER TABLE test1 ADD COLUMN h VARCHAR(2);
ALTER TABLE test1 ADD COLUMN m VARCHAR(2);
ALTER TABLE test1 ADD COLUMN s VARCHAR(2);

-- 4. update the new columns (209 seconds for 985500 rows)
UPDATE test1
SET yy = t.yy, mm = t.mm, dd = t.dd, h = t.h, m = t.m, s = t.s
FROM (  SELECT oid, cur_t, 
              SUBSTRING(test1.cur_t FROM  1 FOR 4) yy, 
              SUBSTRING(test1.cur_t FROM  5 FOR 2) mm,
              SUBSTRING(test1.cur_t FROM  7 FOR 2) dd,
              SUBSTRING(test1.cur_t FROM  9 FOR 2) h,
              SUBSTRING(test1.cur_t FROM 11 FOR 2) m,
              SUBSTRING(test1.cur_t FROM 13 FOR 2) s
FROM test1) t
WHERE t.cur_t = test1.cur_t AND t.oid = test1.oid;

-- 5. function of CREATETime
CREATE OR REPLACE FUNCTION CreateTime(yy VARCHAR(4), mm  VARCHAR(2), dd  VARCHAR(2), h  VARCHAR(2), m  VARCHAR(2), s  VARCHAR(2))
RETURNS TIMESTAMP
AS
$$
SELECT ($1 || '-' || $2 || '-' || $3 || ' ' || $4 || ':' || $5 || ':' || $6)::TIMESTAMP
$$
LANGUAGE SQL;

-- 6. update the new column of ct (135 seconds for 985500 rows)
UPDATE test1
SET ct = CreateTime(yy, mm, dd, h, m, s);

-- 7. drop columns
ALTER TABLE test1 DROP COLUMN cur_t;
ALTER TABLE test1 DROP COLUMN yy;
ALTER TABLE test1 DROP COLUMN mm;
ALTER TABLE test1 DROP COLUMN dd;
ALTER TABLE test1 DROP COLUMN h;
ALTER TABLE test1 DROP COLUMN m;
ALTER TABLE test1 DROP COLUMN s;
--==================================================================================================================--
-- Part-1. Preprocessing
--==================================================================================================================--
-- 0.0. clean the system
DROP TABLE candidate_obj;
DROP TABLE new_obj;
DROP TABLE update_obj;
DROP TABLE delete_obj;
DROP TABLE rhis_traj_rel;
DROP TABLE cur_flowstat;
DROP TABLE hod_flowstat;
DROP TABLE dow_flowstat;
DROP TABLE cur_mobstat;
DROP TABLE hod_mobstat;
DROP TABLE dow_mobstat;
DROP TABLE aff_mov_obj_mobstat;
DROP TABLE aff_mov_obj_movdir; 
DROP TABLE exp_res_roc;
DROP TABLE exp_res_scaling;

-- optional drop queries
DROP TABLE saved_dow_flowstat;
DROP TABLE saved_hod_flowstat;
DROP TABLE saved_dow_mobstat;
DROP TABLE saved_hod_mobstat;

DROP FUNCTION TestPartition(gsize INTEGER, original_point_x DOUBLE PRECISION, original_point_y DOUBLE PRECISION);
DROP FUNCTION congcells(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION);
DROP FUNCTION curflowstat();
DROP FUNCTION curmobstat();
DROP FUNCTION dirangc(his_gid INTEGER, aff_gid INTEGER, con_gid INTEGER);
DROP FUNCTION dircal(x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER);
DROP FUNCTION dircurtocon(cong_gid INTEGER, cong_dir INTEGER, current_gid INTEGER);
DROP FUNCTION distbetweencells(gid1 INTEGER, gid2 INTEGER);
DROP FUNCTION dowflowstat(v_dow INTEGER);
DROP FUNCTION dowmobstat(v_dow INTEGER);
DROP FUNCTION executethesystem(expgroupname TEXT, min_congdev DOUBLE PRECISION, nrTIMESTAMP INTEGER, trajlength INTEGER, detecttimelength INTEGER, w_hod DOUBLE PRECISION,
                              w_dow DOUBLE PRECISION, min_prob_mobstat DOUBLE PRECISION, min_prob_movdir DOUBLE PRECISION);
DROP FUNCTION hodflowstat(v_hod INTEGER);
DROP FUNCTION hodmobstat(v_hod INTEGER);
DROP FUNCTION makegridcellid(gx INTEGER, gy INTEGER);
DROP FUNCTION notifyobjectsmobstat(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION,
                                  min_prob_mobstat DOUBLE PRECISION, c_t TIMESTAMP WITHOUT TIME ZONE);
DROP FUNCTION notifyobjectsmovdir(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION,
                                 min_cos_movdir DOUBLE PRECISION, c_t TIMESTAMP WITHOUT TIME ZONE, traj_length INTEGER);
DROP FUNCTION RocSpaceMobStat(c_t TIMESTAMP, seconds INTEGER, w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION,trajLength INTEGER);
DROP FUNCTION RocSpaceMovDir(c_t TIMESTAMP, seconds INTEGER, w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION, trajLength INTEGER);
DROP FUNCTION AutoExeTheSys();
DROP FUNCTION updaterhistrajrel(nrTIMESTAMP INTEGER, trajlength INTEGER);
DROP FUNCTION InitializeRHisTrajRel(nr_obj INTEGER, traj_length INTEGER, nr_gid INTEGER);
DROP FUNCTION RHisTrajGidCal(gid INTEGER, dir INTEGER, nr_gid INTEGER);
DROP FUNCTION RHisTrajDirCal(previous_dir INTEGER);
DROP FUNCTION UpdateRHisTraj(nr_TIMESTAMP INTEGER, nr_gid INTEGER);
DROP FUNCTION ScalingExperiment(expGroupName TEXT, min_congdev DOUBLE PRECISION, nrTIMESTAMP INTEGER, trajLength INTEGER, 
                               w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, min_prob_mobstat DOUBLE PRECISION, nr_gid_extension INTEGER);
DROP TYPE roc_elements CASCADE;

VACUUM FULL ANALYZE;

SET enable_seqscan = TRUE;

-- 0.1. create plpgsql
-- CREATE LANGUAGE plpgsql;

-- 0.2. this function is to make_grid_cell_id
CREATE OR REPLACE FUNCTION MakeGridCellID(gx INTEGER, gy INTEGER)
RETURNS INTEGER
AS
$$
SELECT $1*10000+$2
$$
LANGUAGE SQL;

-- 0.3. function of current traffic flow direction
CREATE OR REPLACE FUNCTION DirCal(x1 INTEGER, y1 INTEGER, x2 INTEGER, y2 INTEGER)
RETURNS INTEGER AS $$
BEGIN
IF      x1 - x2 = 0   AND y1 - y2 <= -1 THEN RETURN 1;
ELSE IF x1 - x2 <= -1 AND y1 - y2 <= -1 THEN RETURN 2;
ELSE IF x1 - x2 <= -1 AND y1 - y2 = 0   THEN RETURN 3;
ELSE IF x1 - x2 <= -1 AND y1 - y2 >= 1  THEN RETURN 4;
ELSE IF x1 - x2 = 0   AND y1 - y2 >= 1  THEN RETURN 5;
ELSE IF x1 - x2 >= 1  AND y1 - y2 >= 1  THEN RETURN 6;
ELSE IF x1 - x2 >= 1  AND y1 - y2 = 0   THEN RETURN 7;
ELSE IF x1 - x2 >= 1  AND y1 - y2 <= -1 THEN RETURN 8;
ELSE RETURN 0;
END IF; END IF; END IF; END IF;
END IF;	END IF; END IF; END IF;
END;
$$ LANGUAGE 'plpgsql';

-- 0.4. to format the origional table test1 = (oid, x, y, c_time, cur_spd) as test1_entire = (oid, c_gid, c_dir, c_time, c_spd)
CREATE OR REPLACE FUNCTION TestPartition(gsize INTEGER, original_point_x DOUBLE PRECISION, original_point_y DOUBLE PRECISION)
RETURNS void AS $$
BEGIN
-- test1_entire: select oid, c_cell_id, c_time, and dir to do join in with the table of aff_mov_obj to get FP and TP values
CREATE TABLE test1_entire AS
SELECT oid,
      MakeGridCellID((x - $2)::INTEGER/$1 + 1, (y - $3)::INTEGER/$1 + 1) c_gid,
      ct c_time,
      cur_spd c_spd,
      DirCal((x - $2)::INTEGER/$1 + 1, (y - $3)::INTEGER/$1 + 1, ((lag(x) over w1) - $2)::INTEGER/$1 +1, ((lag(y) over w1) - $3)::INTEGER/$1 + 1) AS c_dir
FROM test1
WINDOW w1 AS (partition BY oid ORDER BY ct)
ORDER BY oid, ct;
END;
$$ LANGUAGE 'plpgsql';

-- 0.5. to wisely define the extension and the original point of the study area
-- SELECT max(x) x_max, max(y) y_max, min(x) x_min, min(y) y_min from test1;

-- 0.6. parameters: grid_resolution, original_point_x, originial_point_y
-- SELECT TestPartition(100, 703036.7016, 6169510.299);

-- 0.7. create index for the table of test1_entire
CREATE INDEX idx_test1_entire_oid    ON test1_entire (oid);
CREATE INDEX idx_test1_entire_c_gid  ON test1_entire (c_gid);
CREATE INDEX idx_test1_entire_c_time ON test1_entire (c_time);
CREATE INDEX idx_test1_entire_c_dir  ON test1_entire (c_dir);

--==================================================================================================================--
-- Part-2. Create Tables
--==================================================================================================================--
-- 1.1. candidate_obj stores objects shown in each TIMESTAMP
CREATE TABLE candidate_obj(
oid INTEGER, 
c_gid INTEGER,
c_dir INTEGER, 
c_spd DOUBLE PRECISION
);

-- 1.2. new_obj stores new objects in each TIMESTAMP that is going to be inserted into the table of rhis_traj_rel
CREATE TABLE new_obj(
oid INTEGER, 
c_gid INTEGER,
c_dir INTEGER, 
c_spd DOUBLE PRECISION
);

-- 1.3. update_obj stores already existed objects in each TIMESTAMP that is going to be updated in the table of rhis_traj_rel
CREATE TABLE update_obj(
oid INTEGER, 
c_gid INTEGER,
c_dir INTEGER, 
c_spd DOUBLE PRECISION
);

-- 1.4. delete_obj stores objects that are disappeared in each TIMESTAMP and is going to be deleted from the table of rhis_traj_rel
CREATE TABLE delete_obj(
oid INTEGER
);

-- 1.5. recent historical trajectory relation
CREATE TABLE rhis_traj_rel(
oid INTEGER,
seqnr INTEGER,
gid INTEGER,
dir INTEGER,
cur_spd DOUBLE PRECISION
);

-- create INDEX (perofrmance is almost the same with- and without INDEX)
CREATE INDEX idx_rhis_traj_rel_oid ON rhis_traj_rel(oid);
CREATE INDEX idx_rhis_traj_rel_seqnr ON rhis_traj_rel(seqnr);

-- 1.6. create table of cur_statistic
CREATE TABLE cur_flowstat(
gid INTEGER,
dir INTEGER,
nr INTEGER,
mu DOUBLE PRECISION,
sig DOUBLE PRECISION
);

-- 1.7. create table of dow_statistic
CREATE TABLE dow_flowstat(
gid INTEGER,
dir INTEGER,
dow INTEGER,
nr INTEGER,
mu DOUBLE PRECISION,
sig DOUBLE PRECISION
);

-- 1.8. create table of hod_statistic
CREATE TABLE hod_flowstat(
gid INTEGER,
dir INTEGER,
hod INTEGER,
nr   INTEGER,
mu DOUBLE PRECISION,
sig DOUBLE PRECISION
);

-- 1.9. current mobility statistics
CREATE TABLE cur_mobstat(
dst_gid INTEGER,
dst_dir INTEGER,
src_gid INTEGER,
nr_src2dst INTEGER,
nr_src2any INTEGER
);

-- 1.10. create table for hod_histroical mobility statistics
CREATE TABLE hod_mobstat(
dst_gid INTEGER,
dst_dir INTEGER,
src_gid INTEGER,
hod INTEGER,
nr_src2dst INTEGER,
nr_src2any INTEGER
);

CREATE INDEX idx_hod_mobstat_dst_gid ON hod_mobstat(dst_gid);
CREATE INDEX idx_hod_mobstat_dst_dir ON hod_mobstat(dst_dir);
CREATE INDEX idx_hod_mobstat_src_dir ON hod_mobstat(src_gid);
CREATE INDEX idx_hod_mobstat_hod ON hod_mobstat(hod);

-- 1.11. create TABLE for dow_histroical mobility statistics
CREATE TABLE dow_mobstat(
dst_gid INTEGER,
dst_dir INTEGER,
src_gid INTEGER,
dow INTEGER,
nr_src2dst INTEGER,
nr_src2any INTEGER
);

CREATE INDEX idx_dow_mobstat_dst_gid ON dow_mobstat(dst_gid);
CREATE INDEX idx_dow_mobstat_dst_dir ON dow_mobstat(dst_dir);
CREATE INDEX idx_dow_mobstat_src_dir ON dow_mobstat(src_gid);
CREATE INDEX idx_dow_mobstat_dow ON dow_mobstat(dow);

-- 1.12. CREATE table of affected_moving_objects_mobility_statistic
CREATE TABLE aff_mov_obj_mobstat(
oid INTEGER,
con_gid INTEGER,
con_dir INTEGER,
c_time TIMESTAMP -- for the convenience of ROC space calculation
);

CREATE INDEX idx_aff_mov_obj_mobstat_oid ON aff_mov_obj_mobstat (oid);
CREATE INDEX idx_aff_mov_obj_mobstat_con_cellid ON aff_mov_obj_mobstat (con_gid);
CREATE INDEX idx_aff_mov_obj_mobstat_con_dir ON aff_mov_obj_mobstat (con_dir);
CREATE INDEX idx_aff_mov_obj_mobstat_c_time ON aff_mov_obj_mobstat (c_time);

-- 1.13. CREATE table of affected_moving_objects_movement_directional
CREATE TABLE aff_mov_obj_movdir(
oid INTEGER,
con_gid INTEGER,
con_dir INTEGER,
c_time TIMESTAMP -- for the convenience of ROC space calculation
);

CREATE INDEX idx_aff_mov_obj_movdir_oid ON aff_mov_obj_movdir (oid);
CREATE INDEX idx_aff_mov_obj_movdir_con_cellid ON aff_mov_obj_movdir (con_gid);
CREATE INDEX idx_aff_mov_obj_movdir_con_dir ON aff_mov_obj_movdir (con_dir);
CREATE INDEX idx_aff_mov_obj_movdir_c_time ON aff_mov_obj_movdir (c_time);

--==================================================================================================================--
-- Part-3. Functions
--==================================================================================================================--
-- 2.1. to periodically (in each TIMESTAMP) update the threes tables of new_obj_group, update_obj_group, and delete_obj_group
CREATE OR REPLACE FUNCTION UpdateRHisTrajRel(nrTIMESTAMP INTEGER, trajLength INTEGER)
returns void AS $$
BEGIN
-- truncate three tables
TRUNCATE TABLE candidate_obj, new_obj, update_obj, delete_obj;

-- CREATE a view of candidate_obj that can be used in serveral places, and it will be dropped in the end of this function
INSERT INTO candidate_obj (oid, c_gid, c_dir, c_spd)
SELECT oid, c_gid, c_dir, c_spd
 FROM test1_entire 
WHERE c_time BETWEEN '2012-08-23 07:00:09'::TIMESTAMP + ($1-1) * 10 * '1 second'::INTERVAL
                 AND '2012-08-23 07:00:09'::TIMESTAMP +     $1 * 10 * '1 second'::INTERVAL;
                  
-- group1: new_obj stores new objects in each TIMESTAMP that is going to be inserted into the table of rhis_traj_rel
INSERT INTO new_obj (oid, c_gid, c_dir, c_spd)
SELECT candidate_obj.oid, candidate_obj.c_gid, candidate_obj.c_dir, candidate_obj.c_spd
 FROM candidate_obj 
         LEFT JOIN (SELECT * FROM rhis_traj_rel WHERE seqnr = 1) t1
   ON (t1.oid = candidate_obj.oid)
WHERE t1.gid IS NULL;

-- group2: update_obj stores already existed objects in each TIMESTAMP that is going to be updated in the table of rhis_traj_rel
INSERT INTO update_obj (oid, c_gid, c_dir, c_spd)
SELECT candidate_obj.oid, candidate_obj.c_gid, candidate_obj.c_dir, candidate_obj.c_spd
 FROM candidate_obj, rhis_traj_rel
WHERE candidate_obj.oid = rhis_traj_rel.oid
  AND rhis_traj_rel.seqnr = 1;

-- group3: delete_obj stores objects that are disappeared in each TIMESTAMP and is going to be deleted from the table of rhis_traj_rel
INSERT INTO delete_obj (oid)
SELECT t1.oid
 FROM (SELECT * FROM rhis_traj_rel WHERE seqnr = 1) t1
         LEFT JOIN candidate_obj
           ON (t1.oid = candidate_obj.oid)
WHERE candidate_obj.oid IS NULL;

-- CREATE a view of gid_unchanged_obj to store objects that do not change their current gid (in the update_obj group)
CREATE VIEW gid_unchanged_obj AS 
(SELECT oid, c_spd
  FROM update_obj
 WHERE c_dir = 0);  -- 0 indicates objects are are located in the same grid cell as it was in the last TIMESTAMP

-- CREATE a view of gid_changed_obj to store objects that change their current gid (in the update_obj group)
CREATE VIEW gid_changed_obj AS 
(SELECT oid, c_gid, c_dir, c_spd
  FROM update_obj
 WHERE c_dir <> 0); -- <> 0 indicates objects are are located in the different grid cells

-- insert step1-1: insert new objects (oid, seqnr) to the table of rhis_traj_rel
FOR length IN 1..$2 LOOP
INSERT INTO rhis_traj_rel (oid, seqnr)
SELECT oid, length FROM new_obj;
END LOOP;

-- insert step1-2: update new objects that are shown in the current TIMESTAMP in the table of rhis_traj_rel
UPDATE rhis_traj_rel
  SET gid = t1.c_gid, cur_spd = t1.c_spd, dir = t1.c_dir
    FROM (SELECT * FROM new_obj) t1
    WHERE rhis_traj_rel.seqnr = 1
      AND rhis_traj_rel.oid = t1.oid; -- it is reasonable in the first TIMESTAMP that odi = 0

-- update step2-1: only update the speed if objects are are located in the same grid cell as it was in the last TIMESTAMP
UPDATE rhis_traj_rel
  SET cur_spd = gid_unchanged_obj.c_spd
 FROM gid_unchanged_obj
WHERE rhis_traj_rel.oid = gid_unchanged_obj.oid
  AND rhis_traj_rel.seqnr = 1;

-- update step2-2: shift the seqnr (1, 2, 3, ..., n) -> (2, 3, 4, ..., n+1)
UPDATE rhis_traj_rel
  SET seqnr = seqnr + 1
 FROM gid_changed_obj
WHERE rhis_traj_rel.oid = gid_changed_obj.oid;

-- update step2-3: insert the new-updated object
INSERT INTO rhis_traj_rel(oid, seqnr, gid, dir, cur_spd)
    SELECT oid, 1, c_gid, c_dir, c_spd
      FROM gid_changed_obj;

-- update step2-4: delete the last seqnr of n+1
DELETE FROM rhis_traj_rel WHERE seqnr > $2;

-- delete step3-1: delete objects are disappeared in the current TIMESTAMP from the table of rhis_traj_rel
DELETE FROM rhis_traj_rel WHERE oid IN (SELECT oid FROM delete_obj);

--drop views
DROP VIEW gid_unchanged_obj;
DROP VIEW gid_changed_obj;
END; $$
LANGUAGE 'plpgsql';

-- 2.2. current traffic flow statistics
CREATE OR REPLACE FUNCTION CurFlowStat()
RETURNS void AS $$
BEGIN
-- clean up the table
TRUNCATE TABLE cur_flowstat;

INSERT INTO cur_flowstat (gid, dir, nr, mu, sig)
SELECT gid, dir, count(*), avg(cur_spd), COALESCE(stddev(cur_spd),0)
 FROM rhis_traj_rel
WHERE seqnr = 1
GROUP BY gid, dir;
END; $$
LANGUAGE 'plpgsql';

-- 2.3. dow traffic flow statistics
CREATE OR REPLACE FUNCTION DowFlowStat(v_dow INTEGER)
RETURNS void AS $$
BEGIN
-- group1: update the table of dow_flowstat with the already exist (gid, dir) data set
UPDATE	dow_flowstat AS dh
SET	nr = (c.nr+dh.nr), 
       mu = (c.nr*c.mu+dh.nr*dh.mu)/(c.nr + dh.nr), 
       sig = sqrt((dh.nr*(dh.sig^2)+c.nr*(c.sig^2))/(dh.nr+c.nr)+dh.nr*c.nr*(dh.sig-c.sig)^2/(dh.nr+c.nr)^2)
FROM cur_flowstat AS c
WHERE dh.dow = $1
 --and c.nr > 1
 AND c.gid = dh.gid
 AND c.dir = dh.dir;

-- group2: insert the new (gid, dir) data set into the table of dow_flowstat
INSERT INTO dow_flowstat (gid, dir, dow, nr, mu, sig)
SELECT c.gid, c.dir, $1, c.nr, c.mu, c.sig
 FROM cur_flowstat c
         LEFT JOIN (SELECT * FROM dow_flowstat) dh
   ON (dh.gid = c.gid AND dh.dir = c.dir)
WHERE dh.gid IS NULL;
END; $$
LANGUAGE 'plpgsql';

-- 2.4. hod traffic flow statistics
CREATE OR REPLACE FUNCTION HodFlowStat(v_hod INTEGER)
RETURNS void AS $$
BEGIN
-- group1: update the table of hod_flowstat with the already exist (gid, dir) data set
UPDATE	hod_flowstat AS hh
SET	nr = (c.nr+hh.nr), 
       mu = (c.nr*c.mu+hh.nr*hh.mu)/(c.nr + hh.nr), 
       sig = sqrt((hh.nr*(hh.sig^2)+c.nr*(c.sig^2))/(hh.nr+c.nr)+hh.nr*c.nr*(hh.sig-c.sig)^2/(hh.nr+c.nr)^2)
FROM cur_flowstat AS c
WHERE hh.hod = $1
 --and c.nr > 1
 AND c.gid = hh.gid
 AND c.dir = hh.dir;

-- group2: insert the new (gid, dir) data set into the table of hod_flowstat
INSERT INTO hod_flowstat (gid, dir, hod, nr, mu, sig)
SELECT c.gid, c.dir, $1, c.nr, c.mu, c.sig
 FROM cur_flowstat c
         LEFT JOIN (SELECT * FROM hod_flowstat) hh
   ON (hh.gid = c.gid AND hh.dir = c.dir)
WHERE hh.gid IS NULL;

END; $$
LANGUAGE 'plpgsql';

-- 2.5. congested cells
CREATE OR REPLACE FUNCTION CongCells(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION)
RETURNS TABLE(gid INTEGER, dir INTEGER)
AS $$
SELECT hh.gid AS gid, 
      hh.dir AS dir
FROM   hod_flowstat AS hh, 
      dow_flowstat AS dh,
      cur_flowstat AS ch
WHERE  hh.hod = $3
AND    dh.dow = $4
AND    hh.gid = ch.gid 
AND    hh.dir = ch.dir
AND    hh.gid = dh.gid 
AND    hh.dir = dh.dir
AND    ch.nr >= 2   -- for a 100m*100m grid and for each 10 seconds, currently there are at least five vehicles to CREATE a traffic jam	
AND    ch.mu <= ((hh.mu  * $1 + dh.mu  * $2) / ($1+$2)) - ($5 * (hh.sig * $1 + dh.sig * $2) / ($1 + $2)); -- keep "="!!! It will make hug difference.
$$
LANGUAGE SQL;

-- 2.6. current mobility statistics
CREATE OR REPLACE FUNCTION CurMobStat()
RETURNS void AS $$
BEGIN
	TRUNCATE TABLE cur_mobstat;
	INSERT INTO cur_mobstat (dst_gid, dst_dir, src_gid, nr_src2dst, nr_src2any)
	SELECT  s2d.*, s2a.nr_src2any
	FROM	(SELECT dst.dst_gid, dst.dst_dir, src.src_gid, count(*) AS nr_srs2dst
	         FROM  (SELECT oid, gid AS dst_gid, dir AS dst_dir
	                FROM   rhis_traj_rel
	                WHERE  seqnr = 1) AS dst,
	        (SELECT oid, gid AS src_gid
	         FROM   rhis_traj_rel
	         WHERE  seqnr > 1) AS src
	WHERE  dst.oid = src.oid
	GROUP BY dst.dst_gid, dst.dst_dir, src.src_gid) AS s2d,
	(SELECT gid AS src_gid, COUNT(*) AS nr_src2any
	 FROM    rhis_traj_rel
	 WHERE   seqnr > 1
	 GROUP BY gid) AS s2a
	 WHERE   s2d.src_gid = s2a.src_gid;
END;
$$ LANGUAGE 'plpgsql';

-- 2.7. hod_historical mobitlity statistics
CREATE OR REPLACE FUNCTION HodMobStat(v_hod INTEGER)
RETURNS void AS $$
BEGIN	
-- group1: update the table of his_mobstat with the already exist (dst_gid, dst_dir, src_gid) data set
UPDATE hod_mobstat AS hs
SET    nr_src2dst = hs.nr_src2dst + cs.nr_src2dst, 
      nr_src2any = hs.nr_src2any + cs.nr_src2any
FROM   cur_mobstat AS cs
WHERE  hs.hod = $1
AND    cs.dst_gid = hs.dst_gid
AND    cs.dst_dir = hs.dst_dir
AND    cs.src_gid = hs.src_gid;

-- group2: insert into the table of his_mobstat with the new (dst_gid, dst_dir, src_gid) data set
INSERT INTO hod_mobstat (dst_gid, dst_dir, src_gid, hod, nr_src2dst, nr_src2any)
SELECT      cs.dst_gid, cs.dst_dir, cs.src_gid, $1, cs.nr_src2dst, cs.nr_src2any
FROM	    cur_mobstat AS cs
LEFT JOIN   (SELECT * FROM hod_mobstat) AS hs
ON          (cs.dst_gid = hs.dst_gid AND cs.dst_dir = hs.dst_dir AND cs.src_gid = hs.src_gid)
WHERE       hs.dst_gid IS NULL;	
END;
$$ LANGUAGE 'plpgsql';

-- 2.8. dow_historical mobitlity statistics
CREATE OR REPLACE FUNCTION DowMobStat(v_dow INTEGER)
RETURNS void AS $$
BEGIN	
-- group1: update the table of his_mobstat with the already exist (dst_gid, dst_dir, src_gid) data set
UPDATE dow_mobstat AS ds
SET    nr_src2dst = ds.nr_src2dst + cs.nr_src2dst, 
      nr_src2any = ds.nr_src2any + cs.nr_src2any
FROM   cur_mobstat AS cs
WHERE  ds.dow = $1
AND    cs.dst_gid = ds.dst_gid
AND    cs.dst_dir = ds.dst_dir
AND    cs.src_gid = ds.src_gid;

-- group2: insert into the table of his_mobstat with the new (dst_gid, dst_dir, src_gid) data set
INSERT INTO dow_mobstat (dst_gid, dst_dir, src_gid, dow, nr_src2dst, nr_src2any)
SELECT      cs.dst_gid, cs.dst_dir, cs.src_gid, $1, cs.nr_src2dst, cs.nr_src2any
FROM	    cur_mobstat AS cs
LEFT JOIN   (SELECT * FROM dow_mobstat) AS ds
ON          (cs.dst_gid = ds.dst_gid AND cs.dst_dir = ds.dst_dir AND cs.src_gid = ds.src_gid)
WHERE       ds.dst_gid IS NULL;	
END;
$$ LANGUAGE 'plpgsql';

-- 2.9. notified objects by the mobility statistic criterion
CREATE OR REPLACE FUNCTION NotifyObjectsMobStat(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, 
                                                 min_condiv DOUBLE PRECISION, min_prob_mobstat DOUBLE PRECISION, c_t TIMESTAMP)
RETURNS void AS $$
INSERT INTO aff_mov_obj_mobstat (oid, con_gid, con_dir, c_time)
SELECT traj.oid, con.gid AS con_gid, con.dir AS con_dir, $7 
FROM   hod_mobstat AS hh, 
      dow_mobstat AS dh,
      rhis_traj_rel AS traj,
      CongCells($1, $2, $3, $4, $5) AS con
WHERE  hh.hod = $3 -- match the given projection
AND    dh.dow = $4 -- match the given projection
AND    hh.src_gid = traj.gid -- pattern's source has to match object's current location or gid (i.e., seqnr = 1)
AND    traj.seqnr = 1 -- current location of object (i.e., seqnr = 1)
AND    hh.dst_gid = con.gid -- pattern's destination has to match congested cell's location or gid
AND    hh.dst_dir = con.dir -- pattern's direction has to match congested cell's direction or dir
AND    hh.dst_gid = dh.dst_gid -- patterns' from different projections have to match spatio-sequentially
AND    hh.dst_dir = dh.dst_dir -- -"-
AND    hh.src_gid = dh.src_gid -- -"-
AND    ((hh.nr_src2dst * $1 + dh.nr_src2dst * $2)/($1+$2)) / ((hh.nr_src2any * $1 + dh.nr_src2any * $2)/($1+$2)) > $6; -- min_prob
$$
language SQL;

-- 2.10. function of getting direction angle from his-mov-tendency to fut-mov-tendency
CREATE OR REPLACE FUNCTION DirAngC(his_gid INTEGER, aff_gid INTEGER, con_gid INTEGER)
RETURNS DOUBLE PRECISION AS $$
DECLARE
x1 INTEGER; y1 INTEGER;
x2 INTEGER; y2 INTEGER;
x3 INTEGER; y3 INTEGER;
hDV_X INTEGER;
hDV_Y INTEGER;
fDV_X INTEGER;
fDV_Y INTEGER;
--cos_val DOUBLE PRECISION;
denominator DOUBLE PRECISION;
BEGIN
x1 = $1/10000; y1 = $1%10000;
x2 = $2/10000; y2 = $2%10000;
x3 = $3/10000; y3 = $3%10000;
hDV_X = x2 - x1;
hDV_Y = y2 - y1;
fDV_X = x3 - x2;
fDV_Y = y3 - y2;
denominator = sqrt(hDV_X^2 + hDV_Y^2) * sqrt(fDV_X^2 + fDV_Y^2);
-- denominator = 0 indicates that any of the two dir-vectors can be zero-vector
-- and current-grid-cell is the same region as affacted-grid-cell
IF denominator = 0 THEN 
RETURN -1;
ELSE 
RETURN (hDV_X * fDV_X + hDV_Y * fDV_Y)/denominator;
END IF;
END;
$$ LANGUAGE 'plpgsql';

-- 2.11. function to calculate the vector between the current grid and the directoinal congestion 
CREATE OR REPLACE FUNCTION DirCurToCon(cong_gid INTEGER, cong_dir INTEGER, current_gid INTEGER)
RETURNS DOUBLE PRECISION AS $$
DECLARE
cong_gid_x INTEGER; cong_gid_y INTEGER;
cong_dir_x INTEGER; cong_dir_y INTEGER;
curr_gid_x INTEGER; curr_gid_y INTEGER;
curr2cong_x INTEGER; curr2cong_y INTEGER;
dire2cong_x INTEGER; dire2cong_y INTEGER;
denominator DOUBLE PRECISION;
BEGIN
cong_gid_x = $1/10000; cong_gid_y = $1%10000;
curr_gid_x = $3/10000; curr_gid_y = $3%10000;

IF      cong_dir = 1 THEN cong_dir_x = cong_gid_x;     cong_dir_y = cong_gid_y + 1;
ELSE IF cong_dir = 2 THEN cong_dir_x = cong_gid_x + 1; cong_dir_y = cong_gid_y + 1;
ELSE IF cong_dir = 3 THEN cong_dir_x = cong_gid_x + 1; cong_dir_y = cong_gid_y;
ELSE IF cong_dir = 4 THEN cong_dir_x = cong_gid_x + 1; cong_dir_y = cong_gid_y - 1;
ELSE IF cong_dir = 5 THEN cong_dir_x = cong_gid_x    ; cong_dir_y = cong_gid_y - 1;
ELSE IF cong_dir = 6 THEN cong_dir_x = cong_gid_x - 1; cong_dir_y = cong_gid_y - 1;
ELSE IF cong_dir = 7 THEN cong_dir_x = cong_gid_x - 1; cong_dir_y = cong_gid_y;
ELSE IF cong_dir = 8 THEN cong_dir_x = cong_gid_x - 1; cong_dir_y = cong_gid_y + 1;
ELSE                      cong_dir_x = cong_gid_x;     cong_dir_y = cong_gid_y;
END IF; END IF; END IF; END IF;
END IF;	END IF; END IF; END IF;

curr2cong_x = cong_gid_x - curr_gid_x; 
curr2cong_y = cong_gid_y - curr_gid_y;
dire2cong_x = cong_gid_x - cong_dir_x; 
dire2cong_y = cong_gid_x - cong_dir_y;

denominator = sqrt(curr2cong_x^2 + curr2cong_y^2) * sqrt(dire2cong_x^2 + dire2cong_y^2);
-- denominator = 0 indicates that any of the two dir-vectors can be zero-vector
-- and current-grid-cell is the same region as affacted-grid-cell
IF denominator = 0 THEN 
RETURN -1;
ELSE 
RETURN (curr2cong_x * dire2cong_x + curr2cong_y * dire2cong_y)/denominator;
END IF;
END; $$
LANGUAGE 'plpgsql';

-- 2.12. function the distance in grid cells between grid cells gid1 and gid2
CREATE OR REPLACE FUNCTION DistBetweenCells(gid1 INTEGER, gid2 INTEGER)
RETURNS DOUBLE PRECISION AS $$
SELECT sqrt((($1/10000)-($2/10000))*(($1/10000)-($2/10000)) + (($1%10000)-($2%10000))*(($1%10000)-($2%10000)));	
$$
LANGUAGE SQL;

-- 2.13. notified objects by the movement directional criterion
CREATE OR REPLACE FUNCTION NotifyObjectsMovDir(w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, 
                                              min_condiv DOUBLE PRECISION, min_cos_movdir DOUBLE PRECISION, c_t TIMESTAMP, traj_length INTEGER)
RETURNS void
AS $$
INSERT INTO aff_mov_obj_movdir (oid, con_gid, con_dir, c_time)
SELECT traj_cur_gid.oid, con.gid AS con_gid, con.dir AS con_dir, $7
FROM   rhis_traj_rel AS traj_cur_gid,
      rhis_traj_rel AS traj_his_gid,
      (SELECT oid, max(seqnr) seqnr
       FROM rhis_traj_rel
       WHERE gid is NOT NULL
         AND seqnr > 1
       GROUP BY oid) end_traj,
      CongCells($1, $2, $3, $4, $5) AS con
WHERE  traj_cur_gid.oid = traj_his_gid.oid
 AND  traj_cur_gid.seqnr = 1
 AND  traj_his_gid.seqnr = end_traj.seqnr
 AND  DirAngC(traj_his_gid.gid, traj_cur_gid.gid, con.gid) > $6  -- min_cos
 --   AND  DirCurToCon(con.gid, con.dir, traj_cur_gid.gid) > $6
 AND  DistBetweenCells(traj_cur_gid.gid,con.gid) < sqrt(2)*$8
GROUP BY traj_cur_gid.oid, con_gid, con_dir;
$$ 
LANGUAGE SQL;

--==================================================================================================================--
-- Part-4. Notifcation Accuracy Experiment in term of the ROC space
--==================================================================================================================--
-- 3.1. CREATE a table to save all the experimental results
CREATE TABLE exp_res_roc(
group_name TEXT, min_cong_dev DOUBLE PRECISION, nr_total_ts INTEGER, traj_len INTEGER, det_t_len INTEGER, 
wei_hod DOUBLE PRECISION, wei_dow DOUBLE PRECISION, min_prob_mob_stat DOUBLE PRECISION, min_cos_mov_dir DOUBLE PRECISION,
ts INTEGER, c_t TIMESTAMP, nr_c INTEGER, nr_o INTEGER,
t_cur_flowstat INTERVAL, t_hod_flowstat INTERVAL, t_dow_flowstat INTERVAL,
t_cur_mobstat INTERVAL, t_hod_mobstat INTERVAL, t_dow_mobstat INTERVAL,
t_aff_mov_obj_mobstat INTERVAL, t_aff_mov_obj_movdir INTERVAL,
t_m_s INTERVAL, t_m_d INTERVAL, 
nr_cur_flowstat INTEGER, nr_hod_flowstat INTEGER, nr_dow_flowstat INTEGER,
nr_cur_mobstat INTEGER, nr_hod_mobstat INTEGER, nr_dow_mobstat INTEGER,
nr_aff_mov_obj_mobstat INTEGER, nr_aff_mov_obj_movdir INTEGER,
tp_m_s INTEGER, fp_m_s INTEGER, tn_m_s INTEGER, fn_m_s INTEGER, tpr_m_s DOUBLE PRECISION, fpr_m_s DOUBLE PRECISION,
tp_m_d INTEGER, fp_m_d INTEGER, tn_m_d INTEGER, fn_m_d INTEGER, tpr_m_d DOUBLE PRECISION, fpr_m_d DOUBLE PRECISION
);

-- 3.2. CREATE a new data type
CREATE TYPE roc_elements AS (tp INTEGER, fp INTEGER, fn INTEGER, tn INTEGER, tpr DOUBLE PRECISION, fpr DOUBLE PRECISION);

-- 3.3. function of ROC space for movement directional
CREATE OR REPLACE FUNCTION RocSpaceMovDir
(c_t TIMESTAMP, seconds INTEGER, w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION, trajLength INTEGER)
RETURNS roc_elements AS $$
DECLARE
roc_md_elements roc_elements;
BEGIN	
-- # of true positive = correctly identified / notified (TP)
SELECT count(*) INTO roc_md_elements.tp
 FROM test1_entire AS t, aff_mov_obj_movdir AS n
WHERE t.oid = n.oid
  AND t.c_gid = n.con_gid
  AND t.c_dir = n.con_dir
  AND n.c_time = $1
  AND t.c_time between $1 AND $1 + $2 * '1 second'::INTERVAL;

-- # of false positive = incorrectly identified / notified (FP)
SELECT (count(*) - roc_md_elements.tp) INTO roc_md_elements.fp
 FROM aff_mov_obj_movdir AS a
WHERE a.c_time = $1;
  
-- # of false negative = incorrectly rejected / not notified (FN)
SELECT (count(*) - roc_md_elements.tp) INTO roc_md_elements.fn
 FROM test1_entire AS t, 
      CongCells($3, $4, $5, $6, $7) AS c, 
      (SELECT DISTINCT oid FROM rhis_traj_rel) AS o
WHERE t.c_gid = c.gid
  AND t.c_dir = c.dir
  AND t.oid = o.oid
  AND t.c_time BETWEEN $1 AND $1 + $2 * '1 second'::INTERVAL;

-- # of true negative = correctly rejected / not notified (TN)
SELECT (count(*) - roc_md_elements.tp - roc_md_elements.fp - roc_md_elements.fn) INTO roc_md_elements.tn
 FROM CongCells($3, $4, $5, $6, $7) AS c,
      (SELECT gid FROM rhis_traj_rel WHERE seqnr = 1) AS o
WHERE DistBetweenCells(c.gid, o.gid) < 3*$8;

-- # tpr
IF (roc_md_elements.tp + roc_md_elements.fn) > 0 THEN
SELECT roc_md_elements.tp::DOUBLE PRECISION/(roc_md_elements.tp + roc_md_elements.fn)::DOUBLE PRECISION INTO roc_md_elements.tpr;
ELSE
roc_md_elements.tpr := -1;
END IF;

-- # fpr
IF (roc_md_elements.fp + roc_md_elements.tn) > 0 THEN
SELECT roc_md_elements.fp::DOUBLE PRECISION/(roc_md_elements.fp + roc_md_elements.tn)::DOUBLE PRECISION INTO roc_md_elements.fpr;
ELSE
roc_md_elements.fpr := -1;
END IF;

RETURN roc_md_elements;
END;
$$ language 'plpgsql';

-- 3.4. function of ROC space for mobility statistics
CREATE OR REPLACE FUNCTION RocSpaceMobStat
(c_t TIMESTAMP, seconds INTEGER, w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, v_hod INTEGER, v_dow INTEGER, min_condiv DOUBLE PRECISION, trajLength INTEGER)
RETURNS roc_elements AS $$
DECLARE
roc_ms_elements roc_elements;
BEGIN	
-- # of true positive = correctly identified / notified (TP)
SELECT COUNT(*) INTO roc_ms_elements.tp
 FROM test1_entire AS t, aff_mov_obj_mobstat AS n
WHERE t.oid = n.oid
  AND t.c_gid = n.con_gid
  AND t.c_dir = n.con_dir
  AND n.c_time = $1
  AND t.c_time between $1 AND $1 + $2 * '1 second'::INTERVAL;

-- # of false positive = incorrectly identified / notified (FP)
SELECT (COUNT(*) - roc_ms_elements.tp) INTO roc_ms_elements.fp
 FROM aff_mov_obj_mobstat AS a
WHERE a.c_time = $1;
  
-- # of false negative = incorrectly rejected / not notified (FN)
SELECT (COUNT(*) - roc_ms_elements.tp) INTO roc_ms_elements.fn
 FROM test1_entire AS t, 
      CongCells($3, $4, $5, $6, $7) AS c, 
      (SELECT DISTINCT oid FROM rhis_traj_rel) AS o
WHERE t.c_gid = c.gid
  AND t.c_dir = c.dir
  AND t.oid = o.oid
  AND t.c_time BETWEEN $1 AND $1 + $2 * '1 second'::INTERVAL;

-- # of true negative = correctly rejected / not notified (TN)
SELECT (COUNT(*) - roc_ms_elements.tp - roc_ms_elements.fp - roc_ms_elements.fn) INTO roc_ms_elements.tn
 FROM CongCells($3, $4, $5, $6, $7) AS c,
      (SELECT gid FROM rhis_traj_rel WHERE seqnr = 1) AS o
WHERE DistBetweenCells(c.gid, o.gid) < 3*$8;

-- # tpr
IF (roc_ms_elements.tp + roc_ms_elements.fn) > 0 THEN
SELECT roc_ms_elements.tp::DOUBLE PRECISION/(roc_ms_elements.tp + roc_ms_elements.fn)::DOUBLE PRECISION INTO roc_ms_elements.tpr;
ELSE
roc_ms_elements.tpr := -1;
END IF;

-- # fpr
IF (roc_ms_elements.fp + roc_ms_elements.tn) > 0 THEN
SELECT roc_ms_elements.fp::DOUBLE PRECISION/(roc_ms_elements.fp + roc_ms_elements.tn)::DOUBLE PRECISION INTO roc_ms_elements.fpr;
ELSE
roc_ms_elements.fpr := -1;
END IF;

RETURN roc_ms_elements;
END;
$$ LANGUAGE 'plpgsql';

-- 3.5. for automatic run the function of ExecuteTheSystem
CREATE OR REPLACE FUNCTION ExecuteTheSystem
(expGroupName TEXT, min_congdev DOUBLE PRECISION, nrTimestamp INTEGER, trajLength INTEGER, detectTimeLength INTEGER, 
w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, min_prob_mobstat DOUBLE PRECISION, min_cos_movdir DOUBLE PRECISION)
RETURNS void AS $$
DECLARE
min_time TIMESTAMP; cur_time TIMESTAMP; last_time TIMESTAMP; v_hod INTEGER; v_dow INTEGER;

nr_obj INTEGER; nr_cong INTEGER;

cnt_cur_flowstat INTEGER; cnt_hod_flowstat INTEGER; cnt_dow_flowstat INTEGER;
cnt_cur_mobstat INTEGER; cnt_hod_mobstat INTEGER; cnt_dow_mobstat INTEGER;

cnt_aff_mov_obj_mobstat INTEGER; cnt_aff_mov_obj_movdir INTEGER;

time_s1 INTERVAL; time_s2 INTERVAL; time_s3 INTERVAL; time_s4 INTERVAL;
time_s5 INTERVAL; time_s6 INTERVAL; time_s7 INTERVAL; time_s8 INTERVAL;
sum_t_ms INTERVAL; sum_t_md INTERVAL;

tp_ms INTEGER; fp_ms INTEGER; tn_ms INTEGER; fn_ms INTEGER; tpr_ms DOUBLE PRECISION; fpr_ms DOUBLE PRECISION;
tp_md INTEGER; fp_md INTEGER; tn_md INTEGER; fn_md INTEGER; tpr_md DOUBLE PRECISION; fpr_md DOUBLE PRECISION;
BEGIN
SELECT INTO min_time * FROM (SELECT min(ct) FROM test1) h;
     
FOR m IN 1..$3 LOOP
cur_time = min_time + (m - 1) * 10 * '1 second'::INTERVAL;
SELECT INTO v_hod * FROM (SELECT EXTRACT(HOUR FROM cur_time)) h;
SELECT INTO v_dow * FROM (SELECT EXTRACT( DOW FROM cur_time)) h;
perform UpdateRHisTrajRel(m,$4);

last_time = timeofday()::TIMESTAMP;
perform CurFlowStat();
time_s1 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform HodFlowStat(v_hod);
time_s2 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform DowFlowStat(v_dow);
time_s3 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform CurMobStat();
time_s4 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform HodMobStat(v_hod);
time_s5 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform DowMobStat(v_dow);
time_s6 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform NotifyObjectsMobStat($6, $7, v_hod, v_dow, $2, $8, cur_time);
time_s7 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform NotifyObjectsMovDir($6, $7, v_hod, v_dow, $2, $9, cur_time, $4);
time_s8 = timeofday()::TIMESTAMP-last_time;

sum_t_ms = time_s1 + time_s2 + time_s3 + time_s4 + time_s5 + time_s6 + time_s7;
sum_t_md = time_s1 + time_s2 + time_s3 + time_s8;

SELECT COUNT(DISTINCT oid) INTO nr_obj FROM rhis_traj_rel;
SELECT COUNT(*) INTO nr_cong FROM CongCells($6, $7, v_hod, v_dow, $2);
SELECT COUNT(*) INTO cnt_cur_flowstat FROM cur_flowstat;
SELECT COUNT(*) INTO cnt_hod_flowstat FROM hod_flowstat;
SELECT COUNT(*) INTO cnt_dow_flowstat FROM dow_flowstat;
SELECT COUNT(*) INTO cnt_cur_mobstat FROM cur_mobstat;
SELECT COUNT(*) INTO cnt_hod_mobstat FROM hod_mobstat;
SELECT COUNT(*) INTO cnt_dow_mobstat FROM dow_mobstat;
SELECT COUNT(*) INTO cnt_aff_mov_obj_mobstat FROM aff_mov_obj_mobstat WHERE c_time = cur_time;
SELECT COUNT(*) INTO cnt_aff_mov_obj_movdir FROM aff_mov_obj_movdir WHERE c_time = cur_time;
SELECT tp, fp, fn, tn, tpr, fpr INTO tp_md, fp_md, fn_md, tn_md, tpr_md, fpr_md FROM RocSpaceMovDir(cur_time, $5, $6, $7, v_hod, v_dow, $2, $4);
SELECT tp, fp, fn, tn, tpr, fpr INTO tp_ms, fp_ms, fn_ms, tn_ms, tpr_ms, fpr_ms FROM RocSpaceMobStat(cur_time, $5, $6, $7, v_hod, v_dow, $2, $4);

-- raise notice
RAISE NOTICE '% % % % % % % % % %', cur_time, nr_obj, time_s1, time_s2, time_s3, time_s4, time_s5, time_s6, time_s7, time_s8;

-- save the experimental results INTO the table
INSERT INTO exp_res_roc(
   group_name, min_cong_dev, nr_total_ts, traj_len, det_t_len, 
   wei_hod, wei_dow, min_prob_mob_stat, min_cos_mov_dir,
   ts, c_t, nr_c, nr_o,
   t_cur_flowstat, t_hod_flowstat, t_dow_flowstat,
   t_cur_mobstat, t_hod_mobstat, t_dow_mobstat,
   t_aff_mov_obj_mobstat, t_aff_mov_obj_movdir,
   t_m_s, t_m_d, 
   nr_cur_flowstat, nr_hod_flowstat, nr_dow_flowstat,
   nr_cur_mobstat, nr_hod_mobstat, nr_dow_mobstat,
   nr_aff_mov_obj_mobstat, nr_aff_mov_obj_movdir,
   tp_m_s, fp_m_s, tn_m_s, fn_m_s, tpr_m_s, fpr_m_s,
   tp_m_d, fp_m_d, tn_m_d, fn_m_d, tpr_m_d, fpr_m_d)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9,
      m, cur_time, nr_cong, nr_obj,
      time_s1, time_s2, time_s3, time_s4, time_s5, time_s6, time_s7, time_s8, sum_t_ms, sum_t_md, 
      cnt_cur_flowstat, cnt_hod_flowstat, cnt_dow_flowstat,
      cnt_cur_mobstat, cnt_hod_mobstat, cnt_dow_mobstat,
      cnt_aff_mov_obj_mobstat, cnt_aff_mov_obj_movdir,
      tp_ms, fp_ms, tn_ms, fn_ms, tpr_ms, fpr_ms,
      tp_md, fp_md, tn_md, fn_md, tpr_md, fpr_md);
END LOOP;
END; $$
LANGUAGE 'plpgsql';

-- 3.6. beofre run the following query, run the query according to the particular requirement
TRUNCATE TABLE dow_flowstat, dow_mobstat, hod_flowstat, hod_mobstat;
TRUNCATE TABLE rhis_traj_rel, aff_mov_obj_mobstat, aff_mov_obj_movdir;
TRUNCATE TABLE exp_res_roc;

-- 3.7. automatic query
-- p1: experimental group name
-- p2: minimum congestion deviation
-- p3: number of TIMESTAMPs to be executed
-- p4: trajectory length
-- p5: time length (in seconds) to detect the ROC_SPACE in the future
-- p6: weight of hod
-- p7: weight of dow
-- p8: minimum sending-notification posibility for mobstat(min_prob)
-- p9: minimum sending-notification posibility for movdir(min_cos)
SELECT ExecuteTheSystem('active_learning', 2.0, 5, 10, 120, 0.8, 0.2, 0.2, 0.2);

-- 3.8. function for the accuracy experiment
CREATE OR REPLACE FUNCTION AutoExeTheSys()
RETURNS void AS $$
BEGIN
TRUNCATE TABLE dow_flowstat, dow_mobstat, hod_flowstat, hod_mobstat, rhis_traj_rel, aff_mov_obj_mobstat, aff_mov_obj_movdir;
perform ExecuteTheSystem('active_learning', 2.0, 5, 10, 120, 0.8, 0.2, 0.2, 0.2);
CREATE TABLE saved_dow_flowstat AS SELECT * FROM dow_flowstat;
CREATE TABLE saved_hod_flowstat AS SELECT * FROM hod_flowstat;
CREATE TABLE saved_dow_mobstat AS SELECT * FROM dow_mobstat;
CREATE TABLE saved_hod_mobstat AS SELECT * FROM hod_mobstat;
TRUNCATE TABLE aff_mov_obj_mobstat, aff_mov_obj_movdir;

-- min_prob_mobstat -> (0.1, 0.2, ..., 0.9)
FOR p8 IN 1..1 LOOP
perform ExecuteTheSystem('batch_learning', 2.0, 5, 10, 120, 0.8, 0.2, (p8::DOUBLE PRECISION*0.1::DOUBLE PRECISION), (p8::DOUBLE PRECISION*0.1::DOUBLE PRECISION));
TRUNCATE TABLE dow_flowstat, dow_mobstat, hod_flowstat, hod_mobstat, rhis_traj_rel, aff_mov_obj_mobstat, aff_mov_obj_movdir;
INSERT INTO dow_flowstat (SELECT * FROM saved_dow_flowstat);
INSERT INTO hod_flowstat (SELECT * FROM saved_hod_flowstat);
INSERT INTO dow_mobstat (SELECT * FROM saved_dow_mobstat);
INSERT INTO hod_mobstat (SELECT * FROM saved_hod_mobstat);
END LOOP;
END;
$$ LANGUAGE 'plpgsql';

-- 3.9 run the experiment
SELECT AutoExeTheSys();

-- 3.10. execute PSQL Console with the following code to export the table to the file
COPY exp_res TO 'E:/ExpTemp/active_learning.csv' DELIMITER ',' CSV HEADER
-- or use the following statement in the PSQL Console
\COPY exp_res TO 'C:/Users/glab184/Downloads/Thesis/exp_res.csv' DELIMITER ',' CSV HEADER

-- 3.11. to plot ROC accuracy statistics
SELECT (base.tp_m_s::DOUBLE PRECISION/(base.tp_m_s + base.fn_m_s)::DOUBLE PRECISION) AS tpr_m_s, 
      (base.fp_m_s::DOUBLE PRECISION/(base.fp_m_s + base.tn_m_s)::DOUBLE PRECISION) AS fpr_m_s,
      (base.tp_m_d::DOUBLE PRECISION/(base.tp_m_d + base.fn_m_d)::DOUBLE PRECISION) AS tpr_m_d, 
      (base.fp_m_d::DOUBLE PRECISION/(base.fp_m_d + base.tn_m_d)::DOUBLE PRECISION) AS fpr_m_d, base.*
FROM   (SELECT min_prob_mob_stat, sum(tp_m_s) AS tp_m_s, sum(fp_m_s) AS fp_m_s, sum(tn_m_s) AS tn_m_s, sum(fn_m_s) AS fn_m_s, 
      min_cos_mov_dir, sum(tp_m_d) AS tp_m_d, sum(fp_m_d) AS fp_m_d, sum(tn_m_d) AS tn_m_d, sum(fn_m_d) AS fn_m_d
FROM   exp_res
WHERE  group_name ='batch_learning'
GROUP BY min_prob_mob_stat, min_cos_mov_dir
ORDER BY min_prob_mob_stat) AS base

--==================================================================================================================--
-- Part-5. Random Data Generator for Scaling Experiment
--==================================================================================================================--
-- 5.1. table of scaling_exp_res
CREATE TABLE exp_res_scaling(
group_name TEXT, min_cong_dev DOUBLE PRECISION, nr_ts INTEGER, traj_len INTEGER,
wei_hod DOUBLE PRECISION, wei_dow DOUBLE PRECISION, min_prob_mob_stat DOUBLE PRECISION, nr_gid INTEGER, nr_obj INTEGER,
c_t TIMESTAMP, nr_c INTEGER,
t_cur_flowstat INTERVAL, t_hod_flowstat INTERVAL, t_dow_flowstat INTERVAL,
t_cur_mobstat INTERVAL, t_hod_mobstat INTERVAL, t_dow_mobstat INTERVAL,
t_aff_mov_obj_mobstat INTERVAL, t_m_s INTERVAL,
nr_cur_flowstat INTEGER, nr_hod_flowstat INTEGER, nr_dow_flowstat INTEGER,
nr_cur_mobstat INTEGER, nr_hod_mobstat INTEGER, nr_dow_mobstat INTEGER,
nr_aff_mov_obj_mobstat INTEGER);

-- 5.2. initialize the table of rhis_traj_rel
-- p1: number of objects that will be initialized
-- p2: trajectory length
-- p3: number of grids (e.g. nr_gid = 100 indicates that the extension is 100*100 grid cells)
-- output: <oid, seqner, gid, cur_spd, dir>
-- Particularly dir is an random value with equal possibility. For each row, dir indicates the direction that current object came from.
CREATE OR REPLACE FUNCTION InitializeRHisTrajRel(nr_obj INTEGER, traj_length INTEGER, nr_gid INTEGER)
RETURNS void
AS $$
TRUNCATE TABLE rhis_traj_rel;

INSERT INTO rhis_traj_rel(oid, seqnr)
SELECT oid, seqnr
FROM   generate_series(1,$1) AS oid,
      generate_series(1,$2) AS seqnr;
      
UPDATE rhis_traj_rel
SET    gid = floor(random()*$3 + 1)*10000 + floor(random()*$3 + 1),
      cur_spd = random()*100,
      dir = floor(random() * 8 + 1) -- random INTEGER (1, ..., 8) with equal posibility
WHERE  seqnr = 1;
$$
LANGUAGE SQL;

-- 5.3. calculate dir for the table of rhis_traj_rel
-- p1: based on the previous dir value to calculate the current dir that the object should go with an unequal but realistic possibility
CREATE OR REPLACE FUNCTION RHisTrajDirCal(previous_dir INTEGER)
RETURNS INTEGER AS $$
DECLARE
random_value DOUBLE PRECISION;
current_dir INTEGER;
BEGIN
random_value = random();
IF      random_value <= 0.4 THEN current_dir = $1 % 8;
ELSE IF random_value > 0.4  AND random_value <= 0.55 THEN current_dir = ($1 + 1)  % 8;
ELSE IF random_value > 0.55 AND random_value <= 0.7  THEN current_dir = ($1 + 7)  % 8;
ELSE IF random_value > 0.7  AND random_value <= 0.8  THEN current_dir = ($1 + 2)  % 8;
ELSE IF random_value > 0.8  AND random_value <= 0.9  THEN current_dir = ($1 + 6)  % 8;
ELSE IF random_value > 0.9  AND random_value <= 0.95 THEN current_dir = ($1 + 3)  % 8;
ELSE IF random_value > 0.95 AND random_value <= 1.00 THEN current_dir = ($1 + 5)  % 8;
ELSE IF random_value > 1.00 THEN current_dir = ($1 + 4)  % 8; -- no possibility for repetition
END IF; END IF; END IF; END IF; END IF; END IF; END IF; END IF; 
IF current_dir = 0 THEN
RETURN 8;
ELSE
RETURN current_dir;
END IF;
END; $$
LANGUAGE 'plpgsql';

-- 5.4. calculate gid for the table of rhis_traj_rel
-- p1: previous gid
-- p2: current dir
-- p3: the gid extension for each side
-- output: current gid in a global extension
CREATE OR REPLACE FUNCTION RHisTrajGidCal(gid INTEGER, dir INTEGER, nr_gid INTEGER)
RETURNS INTEGER AS $$
DECLARE
gid_x INTEGER;
gid_y INTEGER;
BEGIN
gid_x = $1 / 10000; 
gid_y = $1 % 10000;
IF      $2 = 1 THEN gid_x = gid_x;     gid_y = gid_y - 1;
ELSE IF $2 = 2 THEN gid_x = gid_x - 1; gid_y = gid_y - 1;
ELSE IF $2 = 3 THEN gid_x = gid_x - 1; gid_y = gid_y;
ELSE IF $2 = 4 THEN gid_x = gid_x - 1; gid_y = gid_y + 1;
ELSE IF $2 = 5 THEN gid_x = gid_x;     gid_y = gid_y + 1;
ELSE IF $2 = 6 THEN gid_x = gid_x + 1; gid_y = gid_y + 1;
ELSE IF $2 = 7 THEN gid_x = gid_x + 1; gid_y = gid_y;
ELSE IF $2 = 8 THEN gid_x = gid_x + 1; gid_y = gid_y - 1;
END IF; END IF; END IF; END IF; END IF; END IF; END IF; END IF; 

-- for global extenstioin
IF      gid_x > $3 THEN gid_x = gid_x % $3;
ELSE IF gid_x < 1  THEN gid_x = $3;
END IF; END IF;

IF      gid_y > $3 THEN gid_y = gid_y % $3;
ELSE IF gid_y < 1  THEN gid_y = $3;
END IF; END IF;

RETURN ((gid_x*10000) + gid_y);
END;
$$
LANGUAGE 'plpgsql';

-- 5.5. update the table of rhis_traj_rel
-- p1: nubmer of timestamps to be estimated
-- p2: the gid extension for each side
CREATE OR REPLACE FUNCTION UpdateRHisTraj(nr_TIMESTAMP INTEGER, nr_gid INTEGER)
RETURNS void
AS $$	
BEGIN
-- shift the seqnr (1, 2, 3, ..., n) -> (2, 3, 4, ..., n+1)
UPDATE rhis_traj_rel SET seqnr = seqnr + 1;

-- insert the new-updated object
INSERT INTO rhis_traj_rel(oid, seqnr, dir, cur_spd)
SELECT oid, 1, RHisTrajDirCal(traj.dir), random()*100
FROM   (SELECT * FROM rhis_traj_rel WHERE seqnr = 2) AS traj
WHERE  oid = traj.oid;

-- update the gid value bASed on the dir value where seqnr = 1
UPDATE rhis_traj_rel AS rt
SET    gid = RHisTrajGidCal(traj.gid, rt.dir, $2)
FROM   rhis_traj_rel AS traj 
WHERE  traj.oid = rt.oid
AND    traj.seqnr = 2
AND    rt.seqnr = 1;

--delete the lASt seqnr of n+1
DELETE FROM rhis_traj_rel WHERE seqnr > $1;
END;
$$ LANGUAGE 'plpgsql';

-- 5.6. execute the scaling experiment
CREATE OR REPLACE FUNCTION ScalingExperiment
(expGroupName TEXT, min_congdev DOUBLE PRECISION, nrTIMESTAMP INTEGER, trajLength INTEGER,
w_hod DOUBLE PRECISION, w_dow DOUBLE PRECISION, min_prob_mobstat DOUBLE PRECISION, nr_gid_extension INTEGER, nr_object INTEGER)
RETURNS void AS $$
DECLARE
min_time TIMESTAMP; cur_time TIMESTAMP; last_time TIMESTAMP; v_hod INTEGER; v_dow INTEGER;

nr_cong INTEGER;

cnt_cur_flowstat INTEGER; cnt_hod_flowstat INTEGER; cnt_dow_flowstat INTEGER;
cnt_cur_mobstat INTEGER; cnt_hod_mobstat INTEGER; cnt_dow_mobstat INTEGER;
cnt_aff_mov_obj_mobstat INTEGER;

time_s1 INTERVAL; time_s2 INTERVAL; time_s3 INTERVAL; time_s4 INTERVAL;
time_s5 INTERVAL; time_s6 INTERVAL; time_s7 INTERVAL; sum_t_ms INTERVAL;

BEGIN
min_time = '2012-08-23 07:00:10'::TIMESTAMP;	
cur_time = min_time + ($3 - 1) * 10 * '1 second'::INTERVAL;

SELECT INTO v_hod * FROM (SELECT EXTRACT(HOUR FROM cur_time)) h;
SELECT INTO v_dow * FROM (SELECT EXTRACT( DOW FROM cur_time)) h;
perform UpdateRHisTraj($4, $8);

last_time = timeofday()::TIMESTAMP;
perform CurFlowStat();
time_s1 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform HodFlowStat(v_hod);
time_s2 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform DowFlowStat(v_dow);
time_s3 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform CurMobStat();
time_s4 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform HodMobStat(v_hod);
time_s5 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;
perform DowMobStat(v_dow);
time_s6 = timeofday()::TIMESTAMP-last_time;

last_time = timeofday()::TIMESTAMP;	
perform NotifyObjectsMobStat($5, $6, v_hod, v_dow, $2, $7, cur_time);
time_s7 = timeofday()::TIMESTAMP-last_time;

sum_t_ms = time_s1 + time_s2 + time_s3 + time_s4 + time_s5 + time_s6 + time_s7;

SELECT count(*) INTO nr_cong FROM CongCells($5, $6, v_hod, v_dow, $2);
SELECT count(*) INTO cnt_cur_flowstat FROM cur_flowstat;
SELECT count(*) INTO cnt_hod_flowstat FROM hod_flowstat;
SELECT count(*) INTO cnt_dow_flowstat FROM dow_flowstat;
SELECT count(*) INTO cnt_cur_mobstat FROM cur_mobstat;
SELECT count(*) INTO cnt_hod_mobstat FROM hod_mobstat;
SELECT count(*) INTO cnt_dow_mobstat FROM dow_mobstat;
SELECT count(*) into cnt_aff_mov_obj_mobstat FROM aff_mov_obj_mobstat WHERE c_time = cur_time;

-- raise notice
RAISE NOTICE '% % % % % % % %', cur_time, time_s1, time_s2, time_s3, time_s4, time_s5, time_s6, time_s7;

-- save the experimental results into the table
INSERT INTO exp_res_scaling(
   group_name, min_cong_dev, nr_ts, traj_len, wei_hod, wei_dow, min_prob_mob_stat, nr_gid, nr_obj,
   c_t, nr_c,
   t_cur_flowstat, t_hod_flowstat, t_dow_flowstat,
   t_cur_mobstat, t_hod_mobstat, t_dow_mobstat,
   t_aff_mov_obj_mobstat, t_m_s,
   nr_cur_flowstat, nr_hod_flowstat, nr_dow_flowstat,
   nr_cur_mobstat, nr_hod_mobstat, nr_dow_mobstat,
   nr_aff_mov_obj_mobstat)
VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9,
      cur_time, nr_cong,
      time_s1, time_s2, time_s3, time_s4, time_s5, time_s6, time_s7, sum_t_ms,
      cnt_cur_flowstat, cnt_hod_flowstat, cnt_dow_flowstat,
      cnt_cur_mobstat, cnt_hod_mobstat, cnt_dow_mobstat,
      cnt_aff_mov_obj_mobstat);
END; $$
LANGUAGE 'plpgsql';

-- 5.7. truncate the table according to the experimental demanding
TRUNCATE TABLE dow_flowstat, dow_mobstat, hod_flowstat, hod_mobstat;
TRUNCATE TABLE rhis_traj_rel, aff_mov_obj_mobstat;
TRUNCATE TABLE exp_res_scaling;

-- 5.8. initialize the trajectory
-- p1: number of moving objects for each TIMESTAMP
-- p2: trajectory length
-- p3: number of grids (e.g. nr_gid = 100 indicates that the extension is 100*100 grid cells)
SELECT InitializeRHisTrajRel(100, 10, 20);

-- 5.9. execute the scaling experiment
-- p1: experimental group name
-- p2: minimum congestion deviation
-- p3: the i-th timestamp that being excuted
-- p4: trajectory length
-- p5: weight of hod
-- p6: weight of dow
-- p7: minimum sending-notification posibility for mobstat(min_prob)
-- p8: number of the grid cells as the extension (100 means 100*100 statistical grid cells -> indicates grid resolution)
-- p9: number of moving objects for each TIMESTAMP
-- also be wise to set the hard-coded parameter in the function of CongCells(...)
SELECT ScalingExperiment('active_learning', 0.01, 3, 10, 0.8, 0.2, 0.1, 20, 100);

-- CREATE table saved_exp_res_scaling_nr_obj_500_2000 as SELECT * FROM exp_res_scaling;
--==================================================================================================================--
-- END
--==================================================================================================================--
TRUNCATE TABLE dow_flowstat, dow_mobstat, hod_flowstat, hod_mobstat;
TRUNCATE TABLE rhis_traj_rel, aff_mov_obj_mobstat;
--TRUNCATE TABLE exp_res_scaling;

SELECT InitializeRHisTrajRel(10000, 4, 40);
SELECT ScalingExperiment('active_learning', 0.01, 1, 10, 0.8, 0.2, 0.1, 40, 10000);
select t_m_s from exp_res_scaling;


select traj_len, 
      sum(nr_c)::double precision/100, 
      sum(nr_aff_mov_obj_mobstat)::double precision/100, 
      sum (nr_cur_flowstat)/100,
      sum (nr_hod_flowstat)/100,
      sum (nr_dow_flowstat)/100,
      sum (nr_cur_mobstat)/100,
      sum (nr_hod_mobstat)/100,
      sum (nr_dow_mobstat)/100,
      sum (t_cur_flowstat)/100,
      sum (t_hod_flowstat)/100,
      sum (t_dow_flowstat)/100,
      sum (t_cur_mobstat)/100,
      sum (t_hod_mobstat)/100,
      sum (t_dow_mobstat)/100
from  saved_exp_res_scaling_traj_length_3_19
group by traj_len
order by traj_len;

select nr_gid, 
      sum(nr_c)::double precision/100, 
      sum(nr_aff_mov_obj_mobstat)::double precision/100, 
      sum (nr_cur_flowstat)/100,
      sum (nr_hod_flowstat)/100,
      sum (nr_dow_flowstat)/100,
      sum (nr_cur_mobstat)/100,
      sum (nr_hod_mobstat)/100,
      sum (nr_dow_mobstat)/100,
      sum (t_cur_flowstat)/100,
      sum (t_hod_flowstat)/100,
      sum (t_dow_flowstat)/100,
      sum (t_cur_mobstat)/100,
      sum (t_hod_mobstat)/100,
      sum (t_dow_mobstat)/100
from  exp_res_scaling
group by nr_gid
order by nr_gid;

--select count(*), nr_gid from exp_res_scaling group by nr_gid;

update exp_res_scaling set nr_obj = nr_obj/100 where nr_obj = 200000;

select sum(t_m_s)/50, nr_obj from exp_res_scaling group by nr_obj;

select (40::double precision/400::double precision), (0.075*400+1);

CREATE TABLE saved_exp_res_scaling AS SELECT * FROM exp_res_scaling;
---------------------------------------------------------------------------
-- Assume that vehicles are moving at an average velocity of 60 km/h in a 40 km by 40 km inner city. For a given number of grid cells, 
-- the experiment investigates in how many vehicles the system can manage to be able to give a 3 minutes warning ahead of congestions.
select 40000::double precision/20::double precision;
select 180::double precision/((40000::double precision/20::double precision)::double precision/(60000::double precision/3600::double precision))::double precision + 1;
-- traj_len = Math.ceil( t_n / ( gl / avg(spd) ) ) + 1
--    nr_grid    g_res   traj_length   nr_managed_obj   nr_congestion
-- 1. 20         2000    3
-- 2. 40         1000    4
-- 3. 80         500     7
-- 4. 100        400     9
-- 5. 200        200     16
-- 6. 400        100     31
