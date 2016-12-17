/*=============================================================================================================
	Pre-preprocessing of cleanning the system and create tables
	October 05, 2015
	Rui Zhu
	Department of Geomatics
	Laval University
	Quebec City, Quebec, Canada
=============================================================================================================*/

CREATE EXTENSION postgis; 

-- step 1. input the data set into the pgAdmin system
-- 1.1. run cmd in windows system=> cmd
-- 1.2. specify the routing=> cd C:\Program Files\PostgreSQL\9.3\bin\ (raster2pgsql.exe is in this document.)
-- 1.3. set the parameters with command line=> 
--      raster2pgsql.exe -s 4326 -I -C -M E:\GZ_Project\GZ_T_Raster_0806 -F -t 250x315 at080623 | psql -U postgres -d gzuhi -h localhost -p 5432

-- 1.4. skip this step for the first time to run the system
DROP TABLE IF EXISTS uhi_union; -- be careful to delete this table!
DROP TABLE IF EXISTS uhi;

DROP FUNCTION IF EXISTS ZR_RasterToMultiPolygons(date_time integer, table_name VARCHAR(8), relative_temp double precision);
DROP FUNCTION IF EXISTS ZR_AutoImportTheData();
DROP FUNCTION IF EXISTS ZR_UHI();

-- step 2. create a table to store the UHI layers in format of the multi-polygon (only one polygon that has many sub-polygons)
CREATE TABLE uhi_union(
	t_s INTEGER,  -- NOTE: this is in integer but will be changed to timestamp later
	rural_temp DOUBLE PRECISION,
	relative_temp DOUBLE PRECISION,
	geom GEOMETRY
);

-- step 3. create a table to store the UHIs
CREATE TABLE uhi(
	t_s TIMESTAMP,
	rural_temp DOUBLE PRECISION,
	relative_temp DOUBLE PRECISION,
	oid SERIAL UNIQUE,
	geom GEOMETRY
);

CREATE INDEX idx_t_s ON uhi(t_s);
CREATE INDEX idx_rural_temp ON uhi(rural_temp);
CREATE INDEX idx_relative_temp ON uhi(relative_temp);
CREATE INDEX idx_oid ON uhi(oid);

--=============================================================================================================--
-- Import the thermal images of UHIs dataset into the system
-- 24(layer each hour) * 7(days) raster layers -> uhi_union in vector -> uhi in vector
--=============================================================================================================--
-- step 4. This function is to transfer UHIs raster image to UHIs vector layer in format of multi-polygons
/*
(1) 'ST_Value(rast, 164, 112, true) AS rural_temp' is specifically defined for the current dataset as the reference 'rural_temp'.
    164 is the column number and 112 is the row number of the input dataset where (0,0) is in the top-left corner.
    Grid(164, 112) of covers the core rural area of the study area - Guangzhou city, China.
(2) Each row in the uhi_union table has multiple UHIs, which are 'relative_temp' degrees higher than the reference 'rural_temp'.
*/
CREATE OR REPLACE FUNCTION ZR_RasterToMultiPolygons(date_time integer, table_name VARCHAR(8), relative_temp double precision)
RETURNS VOID AS $$
BEGIN
	RAISE NOTICE '%', date_time;
	execute
	'INSERT INTO uhi_union(t_s, rural_temp, relative_temp, geom)
	SELECT ' || date_time || ', rural_temp, ' || relative_temp || ', ST_Multi(ST_Union(air_temp.geom)) AS geom
	FROM (SELECT (ST_DumpAsPolygons(rast)).geom  AS geom,
		     (ST_DumpAsPolygons(rast)).val   AS temp, 
		      ST_Value(rast, 164, 112, true) AS rural_temp,
		      filename
	      FROM ' || table_name || ' 
	      WHERE rid = 1) AS air_temp
	WHERE temp >= rural_temp + ' || relative_temp || '
	GROUP BY rural_temp'; 
END; $$
LANGUAGE 'plpgsql';

-- step 5. the function is to import 168 tables (24 layers per day * 7 days) into the table of uhi_union
/*
(1) manually defines the time interval of t_s and run the function ONLY once
    for t_s: 2015073100 ... 2015073123, ..., 2015080600 ... 2015080623
    for relative_temp: 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5
(2) NEVER delete the table of uhi_uion because this calculation is significantly heavy  
*/         
CREATE OR REPLACE FUNCTION ZR_AutoImportTheData()
RETURNS void AS $$
BEGIN
	FOR t_s IN 80600..80623 LOOP
		PERFORM ZR_RasterToMultiPolygons(2015000000 + t_s, 'at0'::VARCHAR(3) || t_s::VARCHAR(5), 5);
	END LOOP;
END;
$$ LANGUAGE 'plpgsql';

SELECT ZR_AutoImportTheData();

--=============================================================================================================--
-- Change the column t_s of uhi_union from "integer" to "timestamp without time zone"
--=============================================================================================================--
-- step 6. change the column name
ALTER TABLE uhi_union RENAME t_s TO ts;

-- step 7. change the column type
ALTER TABLE uhi_union ALTER COLUMN ts TYPE VARCHAR(10);

-- step 8. add columns
ALTER TABLE uhi_union ADD COLUMN oid SERIAL UNIQUE;
ALTER TABLE uhi_union ADD COLUMN ct TIMESTAMP;
ALTER TABLE uhi_union ADD COLUMN yy VARCHAR(4);
ALTER TABLE uhi_union ADD COLUMN mm VARCHAR(2);
ALTER TABLE uhi_union ADD COLUMN dd VARCHAR(2);
ALTER TABLE uhi_union ADD COLUMN h VARCHAR(2);
ALTER TABLE uhi_union ADD COLUMN m VARCHAR(2);
ALTER TABLE uhi_union ADD COLUMN s VARCHAR(2);

-- step 9. update the new columns
UPDATE uhi_union
SET yy = t.yy, mm = t.mm, dd = t.dd, h = t.h, m = 0, s = 1
FROM (  SELECT ts, oid,
	      SUBSTRING(uhi_union.ts FROM  1 FOR 4) yy,
              SUBSTRING(uhi_union.ts FROM  5 FOR 2) mm,
              SUBSTRING(uhi_union.ts FROM  7 FOR 2) dd,
              SUBSTRING(uhi_union.ts FROM  9 FOR 2) h
FROM uhi_union) t
WHERE t.ts = uhi_union.ts AND t.oid = uhi_union.oid;

-- step 10. function of CreateTime
CREATE OR REPLACE FUNCTION CreateTime(yy VARCHAR(4), mm  VARCHAR(2), dd  VARCHAR(2), h  VARCHAR(2), m  VARCHAR(2), s  VARCHAR(2))
RETURNS TIMESTAMP
AS
$$
SELECT ($1 || '-' || $2 || '-' || $3 || ' ' || $4 || ':' || $5 || ':' || $6)::TIMESTAMP
$$
LANGUAGE SQL;

-- step 11. update the new column of ct (135 seconds for 985500 rows)
UPDATE uhi_union
SET ct = CreateTime(yy, mm, dd, h, m, s);

-- step 12. drop columns
ALTER TABLE uhi_union DROP COLUMN oid;
ALTER TABLE uhi_union DROP COLUMN ts;
ALTER TABLE uhi_union DROP COLUMN yy;
ALTER TABLE uhi_union DROP COLUMN mm;
ALTER TABLE uhi_union DROP COLUMN dd;
ALTER TABLE uhi_union DROP COLUMN h;
ALTER TABLE uhi_union DROP COLUMN m;
ALTER TABLE uhi_union DROP COLUMN s;

-- step 13. change the column name
ALTER TABLE uhi_union RENAME COLUMN ct TO t_s;

--=============================================================================================================--
-- Transfer the UHIs from the union type to the independent type
--=============================================================================================================--
-- step 14. the mulit-polygon is divided into several polygons according to the geometries
--          (any two ploygons that are not spatial contiguous are divided as two)
CREATE OR REPLACE FUNCTION ZR_UHI()
RETURNS VOID AS $$
BEGIN
	TRUNCATE TABLE uhi;
	INSERT INTO uhi(rural_temp, relative_temp, t_s, geom)
	SELECT rural_temp, relative_temp, t_s, (ST_Dump(geom)).geom
	FROM uhi_union;
	-- oid is automatically created
END; $$
LANGUAGE 'plpgsql';

-- step 15. insert all the UHIs (in multiple-polygon) from the table of uhi_union into the table of uhi (in single-polygon)
SELECT ZR_UHI(); --NOTE: run this function ONLY once

/*=============================================================================================================
	END
=============================================================================================================*/
VACUUM FULL ANALYZE;
SET enable_seqscan = TRUE;

-- 1. skip this step for the first time to run the system
DROP TABLE IF EXISTS ints_stat;
DROP TABLE IF EXISTS merge_stat;
DROP TABLE IF EXISTS split_stat;
DROP TABLE IF EXISTS uhi_evo;

DROP FUNCTION IF EXISTS ZR_IntsecStat(date_time TIMESTAMP, uhi_intensity DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_Expend(data_time TIMESTAMP, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_Stabilize(data_time TIMESTAMP, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION, low_idx DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_Contract(data_time TIMESTAMP, min_r DOUBLE PRECISION, low_idx DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_PreMergeStat(min_r DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_InhMerge(data_time TIMESTAMP, inh_merge DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_GenMerge(data_time TIMESTAMP, inh_merge DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_PreSplitStat(date_time TIMESTAMP, min_r DOUBLE PRECISION, min_split DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_InhSplit(data_time TIMESTAMP, inh_split DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_GenSplit(data_time TIMESTAMP, inh_split DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_Disappear(date_time TIMESTAMP, min_r DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_Appear(date_time TIMESTAMP, min_r DOUBLE PRECISION);
DROP FUNCTION IF EXISTS ZR_AutoExecuteTheSystem(uhi_intensity DOUBLE PRECISION, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION, 
                        low_idx DOUBLE PRECISION, inh_merge DOUBLE PRECISION, min_split DOUBLE PRECISION, inh_split DOUBLE PRECISION);

-- step 2. create a table to store the UHI statistic information of each UHI
CREATE TABLE ints_stat(
	c_oid INTEGER,
	p_oid INTEGER,
	c_a DOUBLE PRECISION,
	p_a DOUBLE PRECISION,
	i_a DOUBLE PRECISION
);

CREATE INDEX idx_c1_oid ON ints_stat(c_oid);
CREATE INDEX idx_p1_oid ON ints_stat(p_oid);
CREATE INDEX idx_c1_a ON ints_stat(c_a);
CREATE INDEX idx_p1_a ON ints_stat(p_a);
CREATE INDEX idx_i1_a ON ints_stat(i_a);

-- step 3. create a temporary table to store merge intermediate calculation results to avoid duplicated calculating 
CREATE TABLE merge_stat(
	c_oid INTEGER,
	p_oid INTEGER,
	c_a DOUBLE PRECISION,
	i_a DOUBLE PRECISION
);

CREATE INDEX idx_c2_oid ON merge_stat(c_oid);
CREATE INDEX idx_p2_oid ON merge_stat(p_oid);
CREATE INDEX idx_c2_a ON merge_stat(c_a);
CREATE INDEX idx_i2_a ON merge_stat(i_a);

-- step 4. create a temporary table to store split intermediate calculation results to avoid duplicated calculating 
CREATE TABLE split_stat(
	p_oid INTEGER,
	c_oid INTEGER,
	p_a DOUBLE PRECISION,
	i_a DOUBLE PRECISION
);

CREATE INDEX idx_p3_oid ON split_stat(p_oid);
CREATE INDEX idx_c3_oid ON split_stat(c_oid);
CREATE INDEX idx_p3_a ON split_stat(p_a);
CREATE INDEX idx_i3_a ON split_stat(i_a);

-- step 5. create a table to store the evolution of all the UHIs
CREATE TABLE uhi_evo(
        t_s TIMESTAMP,
        c_oid INTEGER,
        p_oid INTEGER,
        events INTEGER
);

CREATE INDEX uhi_evo_p_oid ON uhi_evo(p_oid);
CREATE INDEX uhi_c_oid ON uhi_evo(c_oid);

--=============================================================================================================--
-- Functions for determinating dynamic behavior of UHIs
--=============================================================================================================--
-- step 6. intersection statistics for any of two polygons that are with the same relative temperature and in a continuous timestamp
CREATE OR REPLACE FUNCTION ZR_IntsecStat(date_time TIMESTAMP, uhi_intensity DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	/*
	There are four cases for the intersection
	(1) UHIs exist in the two consectuive timestamps
	(2) No UHIs exist in the two consectuive timestamps
	(3) UHIs exist in the current but not in the previous timestamp
	(4) UHIs exist in the previous but not in the current timestamp
	*/

	-- 6.1. truncate the table
	TRUNCATE TABLE ints_stat;
	
	-- 6.2. independently insert UHIs in the two consecutive timestamps
	INSERT INTO ints_stat(c_oid, p_oid, c_a, p_a, i_a)
	SELECT uhi_cur.oid, 0, ST_Area(uhi_cur.geom) AS c_a, 0, 0
	FROM   (SELECT * FROM  zone
	        WHERE t_s = date_time
	        AND relative_temp = uhi_intensity) AS uhi_cur;
	        
	INSERT INTO ints_stat(c_oid, p_oid, c_a, p_a, i_a)
	SELECT 0, uhi_pre.oid, 0, ST_Area(uhi_pre.geom) AS p_a, 0
	FROM   (SELECT * FROM  zone
	        WHERE t_s = date_time - 3600 * '1 second'::INTERVAL
	        AND relative_temp = uhi_intensity) AS uhi_pre;  

	-- 6.3. the inserted UHIs are delected if they are UHIs in both the two timestamps (table ints_uhi is not empty)
	--      the inserted UHIs are maintained if UHIs exist only in one timestamp (table ints_uhi is empty)
	DELETE FROM ints_stat
	USING (SELECT uhi_cur.oid AS c_oid,
	              uhi_pre.oid AS p_oid,
	              ST_Area(uhi_cur.geom) AS c_a,
	              ST_Area(uhi_pre.geom) AS p_a,
	              ST_Area(ST_Intersection(uhi_cur.geom, uhi_pre.geom)) AS i_a
	       FROM  (SELECT * 
	              FROM  zone
	              WHERE t_s = date_time
	              AND   relative_temp = uhi_intensity) AS uhi_cur,
	             (SELECT *
	              FROM  zone
	              WHERE t_s = date_time - 3600 * '1 second'::INTERVAL
	              AND   relative_temp = uhi_intensity) AS uhi_pre)
	AS ints_uhi
	WHERE (ints_uhi.c_oid = ints_stat.c_oid OR ints_uhi.p_oid = ints_stat.p_oid);

	-- 6.4 UHIs will be inserted into the table ints_stat if there are UHIs in both timestamps
	--     No UHI will be inserted if UHIs exist only in one timetsmap or does not exist in any
	INSERT INTO ints_stat(c_oid, p_oid, c_a, p_a, i_a)
	SELECT uhi_cur.oid AS c_oid,
	       uhi_pre.oid AS p_oid,
	       ST_Area(uhi_cur.geom) AS c_a,
	       ST_Area(uhi_pre.geom) AS p_a,
	       ST_Area(ST_Intersection(uhi_cur.geom, uhi_pre.geom)) AS i_a
	FROM   (SELECT * FROM  zone
	        WHERE t_s = date_time
	        AND relative_temp = uhi_intensity) AS uhi_cur,
	       (SELECT * FROM  zone
	        WHERE t_s = date_time - 3600 * '1 second'::INTERVAL
	        AND   relative_temp = uhi_intensity) AS uhi_pre
	ORDER BY i_a DESC;
END; $$
LANGUAGE 'plpgsql';

--=============================================================================================================--
-- Create evolutionary functions and evolution process of each UHI is labeled as follows:
-- 1 = expend, 2 = stabilize, 3 = contract, 
-- 4 = inherited merge (for the same object), 5 = inherited merge (for different objects), 6 = generated merge
-- 7 = inherited split (for the same object), 8 = inherited split (for different objects), 9 = generated split
-- 10 = disappear, 11 = appear
--=============================================================================================================--
-- step 7. expend function
CREATE OR REPLACE FUNCTION ZR_Expend(data_time TIMESTAMP, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, ints_stat.c_oid, ints_stat.p_oid, 1 
	FROM   ints_stat,
	      (SELECT count(c_oid) AS cnt, c_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       AND    i_a / p_a >= min_r
	       GROUP BY c_oid) AS maintain_obj      -- to summarize the number of each c_oid that satisifies i_a / p_a >= min_r
	WHERE  maintain_obj.cnt = 1                 -- to select there is only one instant case
	AND    ints_stat.c_oid = maintain_obj.c_oid -- for each c_oid
	AND    ints_stat.p_a > 0
	AND    ints_stat.i_a / ints_stat.p_a >= min_r -- to find the (c_oid and p_oid) that meet this condition
	AND    ints_stat.c_a / ints_stat.p_a >= up_idx;
END; $$
LANGUAGE 'plpgsql';
	       
-- step 8. stabilize function
CREATE OR REPLACE FUNCTION ZR_Stabilize(data_time TIMESTAMP, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION, low_idx DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, ints_stat.c_oid, ints_stat.p_oid, 2 
	FROM   ints_stat,
	      (SELECT count(c_oid) AS cnt, c_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       AND    i_a / p_a >= min_r
	       GROUP BY c_oid) AS maintain_obj 
	WHERE  maintain_obj.cnt = 1
	AND    ints_stat.c_oid = maintain_obj.c_oid
	AND    ints_stat.p_a > 0
	AND    ints_stat.i_a / ints_stat.p_a >= min_r	
	AND    ints_stat.c_a / ints_stat.p_a >= low_idx  
	AND    ints_stat.c_a / ints_stat.p_a < up_idx;  -- stabilize
END; $$
LANGUAGE 'plpgsql';

-- step 9. contract function
CREATE OR REPLACE FUNCTION ZR_Contract(data_time TIMESTAMP, min_r DOUBLE PRECISION, low_idx DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, ints_stat.c_oid, ints_stat.p_oid, 3 
	FROM   ints_stat,
	      (SELECT count(c_oid) AS cnt, c_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       AND    i_a / p_a >= min_r
	       GROUP BY c_oid) AS maintain_obj 
	WHERE  maintain_obj.cnt = 1
	AND    ints_stat.c_oid = maintain_obj.c_oid
	AND    ints_stat.p_a > 0
	AND    ints_stat.i_a / ints_stat.p_a >= min_r
	AND    ints_stat.c_a / ints_stat.p_a < low_idx;  -- contract
END; $$
LANGUAGE 'plpgsql';

-- step 10. function for the merge intermedicate statistics
CREATE OR REPLACE FUNCTION ZR_PreMergeStat(min_r DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	TRUNCATE TABLE merge_stat;
	INSERT INTO merge_stat(c_oid, p_oid, c_a, i_a)
	SELECT ints_stat.c_oid, ints_stat.p_oid, ints_stat.c_a, ints_stat.i_a
	FROM   ints_stat, 
              (SELECT count(c_oid), c_oid
               FROM  (SELECT ints_stat.c_oid
	              FROM   ints_stat
	              WHERE  p_a > 0
	              AND    ints_stat.i_a / ints_stat.p_a >= min_r) AS cur_g
               GROUP BY c_oid) AS cnt_cur_g
        WHERE  cnt_cur_g.count > 1
        AND    cnt_cur_g.c_oid = ints_stat.c_oid
        AND    ints_stat.p_a > 0
        AND    ints_stat.i_a / ints_stat.p_a >= min_r;
END; $$
LANGUAGE 'plpgsql';

-- step 11. inherited merge function
CREATE OR REPLACE FUNCTION ZR_InhMerge(data_time TIMESTAMP, inh_merge DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	-- case 1. inherited merge for the same objects
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, merge_stat.c_oid, merge_stat.p_oid, 4
	FROM   merge_stat,
	      (SELECT c_oid, 
	              max(i_a) AS max_i_a
               FROM   merge_stat
               GROUP BY c_oid) AS p_key               -- the primery key uniquely defines the two intersection UHIs
	WHERE  merge_stat.c_oid = p_key.c_oid         -- find the specific current-UHI in the table of ints_stat
	AND    merge_stat.i_a = p_key.max_i_a
	AND    merge_stat.c_a > 0
	AND    merge_stat.i_a / merge_stat.c_a >= inh_merge; -- to list the inherited merging UHI

	-- case 2. inherited merge for different objects
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, merge_stat.c_oid, merge_stat.p_oid, 5
	FROM   merge_stat,
	      (SELECT merge_stat.c_oid, merge_stat.p_oid
	       FROM   merge_stat,
		     (SELECT c_oid, 
			     max(i_a) AS max_i_a
		      FROM   merge_stat
		      GROUP BY c_oid) AS p_key               -- the primery key uniquely defines the two intersection UHIs
	       WHERE  merge_stat.c_oid = p_key.c_oid       -- find the specific current-UHI in the table of ints_stat
	       AND    merge_stat.i_a = p_key.max_i_a
	       AND    merge_stat.c_a > 0
	       AND    merge_stat.i_a / merge_stat.c_a >= inh_merge) AS inh_mer_obj
	WHERE inh_mer_obj.c_oid = merge_stat.c_oid
	AND   inh_mer_obj.p_oid <> merge_stat.p_oid;
END; $$
LANGUAGE 'plpgsql';

-- step 12. generated merge function
CREATE OR REPLACE FUNCTION ZR_GenMerge(data_time TIMESTAMP, inh_merge DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, merge_stat.c_oid, merge_stat.p_oid, 6
	FROM   merge_stat,
	      (SELECT DISTINCT merge_stat.c_oid -- using DISTINCT is so important to avoid duplicate selection
	       FROM   merge_stat,
	             (SELECT c_oid, 
	                     max(i_a) AS max_i_a
                      FROM   merge_stat
                      GROUP BY c_oid) AS p_key
	       WHERE  merge_stat.c_oid  = p_key.c_oid
	       AND    merge_stat.i_a = p_key.max_i_a
	       AND    merge_stat.c_a > 0
	       AND    merge_stat.i_a / merge_stat.c_a < inh_merge) AS inh_mer_obj
	WHERE inh_mer_obj.c_oid = merge_stat.c_oid;
END; $$
LANGUAGE 'plpgsql';

-- step 13. function to list all the UHIs that will be split
CREATE OR REPLACE FUNCTION ZR_PreSplitStat(date_time TIMESTAMP, min_r DOUBLE PRECISION, min_split DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	-- 13.1 truncate the table
	TRUNCATE TABLE split_stat;
	
	-- 13.2 create a veiw that will be used twice in this function
	CREATE TABLE com_ints_stat AS
        SELECT ints_stat.* 
	FROM   ints_stat
        LEFT JOIN (SELECT c_oid FROM uhi_evo WHERE uhi_evo.t_s = date_time) AS cur_g
        ON    cur_g.c_oid = ints_stat.c_oid
        WHERE ints_stat.i_a > 0
        AND   cur_g.c_oid IS NULL; 
        
        -- 13.3 list all the UHIs that will be split                        
	INSERT INTO split_stat(p_oid, c_oid, p_a, i_a)
	SELECT com_ints_stat.p_oid, com_ints_stat.c_oid, com_ints_stat.p_a, com_ints_stat.i_a
	FROM   com_ints_stat,
	      (SELECT sum(i_a), p_oid                      -- select the sum of intersection area in group of each p_oid
	       FROM  (SELECT com_ints_stat.p_oid, com_ints_stat.i_a  -- select all the intersection areas in group of each p_oid
		      FROM   com_ints_stat, 
			    (SELECT count(p_oid) AS cnt, p_oid
			     FROM   com_ints_stat
			     WHERE  p_a > 0
			     AND    i_a / p_a < min_r
			     GROUP BY p_oid) AS pre_g,        -- count the number of intersections for each p_oid
			    (SELECT count(p_oid) AS cnt, p_oid
			     FROM   com_ints_stat
			     GROUP BY p_oid) AS pre_g_sum			     
		      WHERE  pre_g.cnt = pre_g_sum.cnt       -- to ensure all the i_a / p_a are smaller than min_r
		      AND    pre_g.p_oid = pre_g_sum.p_oid
		      AND    pre_g.cnt > 1                   -- to list the p_oid that has more than two intersections with the c_oid
		      AND    com_ints_stat.i_a > 0           -- to list the p_oid that its intersection area is > 0 (REAL intersection)
		      AND    com_ints_stat.p_a > 0
		      AND    com_ints_stat.i_a / com_ints_stat.p_a < min_r
		      AND    com_ints_stat.p_oid = pre_g.p_oid) AS inters_a_pre_g  -- to link to the p_oid in the table of com_ints_stat
	       GROUP BY inters_a_pre_g.p_oid) AS sum_a_pre_g
	WHERE  sum_a_pre_g.p_oid = com_ints_stat.p_oid
	AND    com_ints_stat.p_a > 0
	AND    sum_a_pre_g.sum / com_ints_stat.p_a >= min_split;
	
	--13.4 drop the view
	DROP TABLE com_ints_stat;
END; $$
LANGUAGE 'plpgsql';

-- step 14. inherited split function
CREATE OR REPLACE FUNCTION ZR_InhSplit(data_time TIMESTAMP, inh_split DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	-- case 1. inherited split for the same objects
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, split_stat.c_oid, split_stat.p_oid, 7
	FROM   split_stat,
	      (SELECT p_oid,
	              max(i_a) AS max_i_a
               FROM   split_stat
               GROUP BY p_oid) AS p_key               -- the primery key uniquely defines the two intersection UHIs
	WHERE  split_stat.p_oid  = p_key.p_oid      -- find the specific current-UHI in the table of ints_stat
	AND    split_stat.i_a = p_key.max_i_a
	-- to list the inherited split UHI (This value MUSR bigger than 0.5 to ensure this is only one inherited split UHI)
	AND    split_stat.p_a > 0
	AND    split_stat.i_a / split_stat.p_a >= inh_split; 

	-- case 2. inherited split for different objects
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, split_stat.c_oid, split_stat.p_oid, 8
	FROM   split_stat,
	      (SELECT split_stat.c_oid, split_stat.p_oid
	       FROM   split_stat,
		     (SELECT p_oid, 
			     max(i_a) AS max_i_a
		      FROM   split_stat
		      GROUP BY p_oid) AS p_key               -- the primery key uniquely defines the two intersection UHIs
	       WHERE  split_stat.p_oid = p_key.p_oid         -- find the specific current-UHI in the table of ints_stat
	       AND    split_stat.i_a = p_key.max_i_a
	       AND    split_stat.p_a > 0
	       AND    split_stat.i_a / split_stat.p_a >= inh_split) AS inh_spl_obj
	WHERE inh_spl_obj.p_oid = split_stat.p_oid
	AND   inh_spl_obj.c_oid <> split_stat.c_oid;
END; $$
LANGUAGE 'plpgsql';

-- step 15. inherited split function
CREATE OR REPLACE FUNCTION ZR_GenSplit(data_time TIMESTAMP, inh_split DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT data_time, split_stat.c_oid, split_stat.p_oid, 9
	FROM   split_stat,
	      (SELECT DISTINCT split_stat.p_oid  -- using DISTINCT is so important to avoid duplicate selection
	       FROM   split_stat,
		      (SELECT p_oid,
			      max(i_a) AS max_i_a
		       FROM   split_stat
		       GROUP BY p_oid) AS p_key              -- the primery key uniquely defines the two intersection UHIs
	       WHERE  split_stat.p_oid  = p_key.p_oid        -- find the specific current-UHI in the table of ints_stat
	       AND    split_stat.i_a = p_key.max_i_a
	       -- to list the generated split UHI (be aware that: inh_split < min_r)
	       AND    split_stat.p_a > 0
	       AND    split_stat.i_a / split_stat.p_a < inh_split) AS inh_spl_obj
	WHERE  inh_spl_obj.p_oid = split_stat.p_oid;
END; $$
LANGUAGE 'plpgsql';

-- step 16. disappear function of processing the disappear-event
CREATE OR REPLACE FUNCTION ZR_Disappear(date_time TIMESTAMP, min_r DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT date_time, 0, target_oid.p_oid, 10
	FROM  (SELECT p_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       GROUP BY p_oid) AS target_oid
	LEFT JOIN
	-- this sub-select clause is for listing the p_oid in the 'grow / stabilize / shrink' event
	-- theorically, there should be ONLY one intersection area that satisfy the 'i_a / p_a >= min_r' in this select caluse
	-- which means that p_oid can either be 'grow / stabilize / shrink' or merged into the intersected c_oid with other p_oid UHIs
	-- therefore, min_r MUST be bigger than 0.5
	      (SELECT p_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       AND    i_a / p_a >= min_r
	       GROUP BY p_oid) AS inters_oid
	ON     target_oid.p_oid = inters_oid.p_oid
	LEFT JOIN
	      (SELECT p_oid FROM merge_stat GROUP BY p_oid) AS merged_oid
	ON     target_oid.p_oid = merged_oid.p_oid
	LEFT JOIN
	      (SELECT p_oid FROM split_stat GROUP BY p_oid) AS split_oid
	ON     target_oid.p_oid = split_oid.p_oid
	WHERE  inters_oid.p_oid IS NULL
	AND    merged_oid.p_oid IS NULL
	AND    split_oid.p_oid  IS NULL;
END; $$
LANGUAGE 'plpgsql';

-- step 17. appear function of processing the appear-event
CREATE OR REPLACE FUNCTION ZR_Appear(date_time TIMESTAMP, min_r DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	INSERT INTO uhi_evo(t_s, c_oid, p_oid, events)
	SELECT date_time, target_oid.c_oid, 0, 11
	FROM  (SELECT c_oid
	       FROM   ints_stat
	       WHERE  c_a > 0
	       GROUP BY c_oid) AS target_oid
	LEFT JOIN
	      (SELECT c_oid
	       FROM   ints_stat
	       WHERE  p_a > 0
	       AND    i_a / p_a >= min_r
	       GROUP BY c_oid) AS inters_oid
	ON     target_oid.c_oid = inters_oid.c_oid
	LEFT JOIN
	      (SELECT c_oid FROM merge_stat GROUP BY c_oid) AS merged_oid
	ON     target_oid.c_oid = merged_oid.c_oid
	LEFT JOIN
	      (SELECT c_oid FROM split_stat GROUP BY c_oid) AS split_oid
	ON     target_oid.c_oid = split_oid.c_oid
	WHERE  inters_oid.c_oid IS NULL
	AND    merged_oid.c_oid IS NULL
	AND    split_oid.c_oid  IS NULL;
END; $$
LANGUAGE 'plpgsql';

--=============================================================================================================--
-- Automatically execute the system to simulate dynamic behaviors of UHIs
--=============================================================================================================--
-- step 18. function to execture the system automatically
CREATE OR REPLACE FUNCTION ZR_AutoExecuteTheSystem
	(uhi_intensity DOUBLE PRECISION, min_r DOUBLE PRECISION, up_idx DOUBLE PRECISION, low_idx DOUBLE PRECISION,
         inh_merge DOUBLE PRECISION, min_split DOUBLE PRECISION, inh_split DOUBLE PRECISION)
RETURNS void AS $$
DECLARE
	t_s TIMESTAMP;
BEGIN
FOR nr IN 1..168 LOOP
	t_s = '2015-07-31 00:00:01'::TIMESTAMP + nr * 3600 * '1 second'::INTERVAL;
	RAISE NOTICE '%', t_s;

	PERFORM ZR_IntsecStat(t_s, uhi_intensity);

	PERFORM ZR_Expend(t_s, min_r, up_idx);
	PERFORM ZR_Stabilize(t_s, min_r, up_idx, low_idx);
	PERFORM ZR_Contract(t_s, min_r, low_idx);

	PERFORM ZR_PreMergeStat(min_r);
	PERFORM ZR_InhMerge(t_s, inh_merge);
	PERFORM ZR_GenMerge(t_s, inh_merge);

	PERFORM ZR_PreSplitStat(t_s, min_r, min_split);  
	PERFORM ZR_InhSplit(t_s, inh_split);
	PERFORM ZR_GenSplit(t_s, inh_split);

	PERFORM ZR_Disappear(t_s, min_r);
	PERFORM ZR_Appear(t_s, min_r);
END LOOP;
END;
$$ LANGUAGE 'plpgsql';

-- step 19. clear the uhi_evo table
TRUNCATE uhi_evo;

-- step 20. run the system
-- 1. uhi_intensity = 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5
-- 2. min_r > 0.5
-- 3. up_idx > 1
-- 4. low_idx < 1
-- 5. inh_merge > 0.5
-- 6. min_split < 1
-- 7. 0.5 < inh_split < min_r
SELECT ZR_AutoExecuteTheSystem(1.5, 0.53, 1.1, 0.9, 0.65, 0.1, 0.5001);

--=====================================================================================================================--
--	END
--=====================================================================================================================--
-- step 1. skip this step for the first time to run the system
DROP TABLE IF EXISTS rein_uhi;
DROP TABLE IF EXISTS uhi_process;

DROP FUNCTION IF EXISTS ZR_Process();
DROP FUNCTION IF EXISTS ZR_Sequence();
DROP FUNCTION IF EXISTS ZR_Reincarnate(app_dth_t INTEGER, min_r DOUBLE PRECISION, min_rein DOUBLE PRECISION);

-- step 2. to store the sequence of each UHI(s)
CREATE TABLE uhi_process(
	obj_id INTEGER,
	pro_seq_id INTEGER,
        t_s TIMESTAMP,
        c_oid INTEGER,
        p_oid INTEGER,
        filiation VARCHAR(13),
        transition VARCHAR(13)
);

CREATE INDEX idx_uhi_process_obj_id ON uhi_process(obj_id);
CREATE INDEX idx_uhi_process_pro_seq_id ON uhi_process(pro_seq_id);
CREATE INDEX idx_uhi_process_p_oid ON uhi_process(p_oid);
CREATE INDEX idx_uhi_process_c_oid ON uhi_process(c_oid);

-- step 3. create a table to store the reincarnated UHIs
CREATE TABLE rein_uhi(
        tail_node INTEGER,
        head_node INTEGER
);

-- step 4. to list the sequence for filiation
CREATE OR REPLACE FUNCTION ZR_Process()
RETURNS VOID AS $$
BEGIN
	TRUNCATE TABLE uhi_process;
	
	INSERT INTO uhi_process(obj_id, t_s, c_oid, p_oid, filiation)
        SELECT max(path) AS path, pros.t_s, pros.c_oid, pros.p_oid, pros.filiation
        FROM  (WITH RECURSIVE pro_u(path, t_s, c_oid, p_oid, filiation)
	       AS (SELECT row_number() OVER (ORDER BY t_s) AS path, -- to get the current row number to create the nr_pro
	                  root.t_s, root.c_oid, root.p_oid, root.filiation
	           FROM   uhi_evo AS root
	           UNION  
	           SELECT leaf.path, root.t_s, root.c_oid, root.p_oid, root.filiation
	           FROM   uhi_evo AS root,
	                  pro_u AS leaf
	           WHERE  root.c_oid = leaf.p_oid
	           AND    leaf.p_oid > 0    -- to avoid the dead circle and to ensure that 'appear' and 'disappear' filiation are included as well
	           AND    leaf.filiation <> 'separa_d_obj'  -- to start the chain when it is inherited split for different objects
	           AND    root.filiation <> 'annexa_d_obj'  -- to stop the chain when it is inherited merge for different objects
	           AND    root.filiation <> 'merging'  -- to stop the chain when it is generated merge
	           AND    root.filiation <> 'splitting') -- to stop the chain when it is generated split
	       SELECT * FROM pro_u
	       ORDER BY path, t_s) AS pros
	GROUP BY pros.t_s, pros.c_oid, pros.p_oid, pros.filiation;
END; $$
LANGUAGE 'plpgsql';

-- step 5. to list the sequence for the filiation
CREATE OR REPLACE FUNCTION ZR_Sequence()
RETURNS VOID AS $$
BEGIN
	-- 5.1. generate series number at the first record for each sequence
	UPDATE uhi_process
	SET    pro_seq_id = series
	FROM  (SELECT *, row_number() OVER (ORDER BY t_s) AS series
               FROM  (SELECT *, lag(filiation) OVER (PARTITION BY obj_id ORDER BY t_s) AS previous_event
                      FROM uhi_process) AS prev_event
               WHERE  previous_event <> filiation
               OR     previous_event IS NULL
               ORDER BY obj_id, t_s) AS temp_t
        WHERE uhi_process.obj_id = temp_t.obj_id
        AND   uhi_process.t_s = temp_t.t_s
        AND   uhi_process.c_oid = temp_t.c_oid
        AND   uhi_process.p_oid = temp_t.p_oid;

	-- 5.2. create a table to insert the first record of all the sequences (when length of sequence > 1)
        CREATE TABLE pro_seq_id_info AS 
	SELECT uhi_process.* FROM uhi_process, uhi_process AS temp_t
	WHERE  uhi_process.t_s = temp_t.t_s - 3600 * '1 second'::INTERVAL
	AND    uhi_process.pro_seq_id IS NOT NULL
	AND temp_t.pro_seq_id IS NULL
	AND temp_t.obj_id = uhi_process.obj_id
	AND temp_t.filiation = uhi_process.filiation
	ORDER BY obj_id, t_s;

	-- 5.3. to insert all the records from the second to the last of all the sequences (when length of sequence > 1)
	INSERT INTO pro_seq_id_info (obj_id, pro_seq_id, t_s, c_oid, p_oid, filiation)
	SELECT obj_id, pro_seq_id, t_s, c_oid, p_oid, filiation
	FROM uhi_process
	WHERE pro_seq_id IS NULL;

	-- 5.4. insert all the pro_seq_id to the uhi_process table based on the pro_seq_id_info table
	UPDATE uhi_process
	SET    pro_seq_id = candidate.pro_seq_id
	FROM  (WITH cte AS (SELECT t1.obj_id, t1.t_s, t1.series, t1.filiation,
                                   COALESCE(t1.pro_seq_id, t2.pro_seq_id, 0) AS pro_seq_id,
                                   rank() OVER (PARTITION BY t1.series ORDER BY t2.series DESC) AS pos
                            FROM  (SELECT row_number() OVER (ORDER BY obj_id, t_s) AS series, * 
                                   FROM   pro_seq_id_info) AS t1
                            LEFT OUTER JOIN (
                                   SELECT row_number() OVER (ORDER BY obj_id, t_s) AS series, *
                                   FROM pro_seq_id_info) AS t2
                            ON  t2.pro_seq_id IS NOT NULL
                            AND t2.series < t1.series)
               SELECT series, obj_id, pro_seq_id, t_s, filiation
               FROM cte
               WHERE pos = 1
               ORDER BY obj_id, t_s) AS candidate
        WHERE uhi_process.obj_id = candidate.obj_id
        AND   uhi_process.filiation = candidate.filiation
        AND   uhi_process.t_s = candidate.t_s;

	-- 5.5. delete the table
        DROP TABLE pro_seq_id_info;
END; $$
LANGUAGE 'plpgsql';

-- step 6. reincarnate function
CREATE OR REPLACE FUNCTION ZR_Reincarnate(app_dth_t INTEGER, min_r DOUBLE PRECISION, min_rein DOUBLE PRECISION)
RETURNS VOID AS $$
BEGIN
	-- 1. truncate the table of rein_uhi
	TRUNCATE TABLE rein_uhi;

	-- 2. summaries the reincarnated candidates of the UHIs
	--    it is possible that one tail_node mapping to several head_node(s) for the resason that there could be several 
	--    max(ints_area) in equal for each tail_node IF min_rein < 0.5
	CREATE TABLE rein_cand AS
	SELECT tail_seq.oid AS tail_node, 
	       head_seq.oid AS head_node,
	      (head_seq.t_s - tail_seq.t_s) AS dth_t,
	       ST_Area(ST_Intersection(head_seq.geom, tail_seq.geom)) AS i_a
	FROM  (SELECT uhi.*
	       FROM   uhi, uhi_process
	       WHERE  filiation = 11
	       AND uhi.oid = uhi_process.c_oid) AS head_seq,  -- create event
	      (SELECT uhi.*
	       FROM   uhi, uhi_process
	       WHERE  uhi_process.filiation = 10
	       AND uhi.oid = uhi_process.p_oid) AS tail_seq   -- disappear event
	WHERE  head_seq.t_s - tail_seq.t_s <= app_dth_t * 3600 * '1 second'::INTERVAL   -- apparent death time app_dth_t
	AND    head_seq.t_s - tail_seq.t_s >= 2 * 3600 * '1 second'::INTERVAL    -- at least be dead for one timestamp
	AND    ST_Area(ST_Intersection(head_seq.geom, tail_seq.geom)) / ST_Area(tail_seq.geom) >= min_r
	AND    ST_Area(ST_Intersection(head_seq.geom, tail_seq.geom)) / ST_Area(head_seq.geom) >= min_rein;

	-- 3. select the reincarnated UHIs
	INSERT INTO rein_uhi(tail_node, head_node)
	SELECT rein_cand.tail_node, rein_cand.head_node
	FROM   rein_cand,
	       -- NOTE: For the tuple of (rein_cand.head_node, rein_cand.tail_node, rein_cand.dth_t), it is possible that 
	       --       one single head_node mapping to several tail_node. While ONLY the min(dth_t) should be selected.
	      (SELECT rein_cand.head_node, min(rein_cand.dth_t) AS min_dth_t
	       FROM   rein_cand, 
		     (SELECT min(dth_t) AS min_dth_t, tail_node
		      FROM rein_cand
		      GROUP BY tail_node) AS dth_cand,
		     (SELECT max(i_a) AS max_i_a, dth_t, tail_node
		      FROM rein_cand
		      GROUP BY tail_node, dth_t) AS ints_cand
	       WHERE  dth_cand.min_dth_t  = rein_cand.dth_t
	       AND    dth_cand.tail_node  = rein_cand.tail_node
	       AND    ints_cand.max_i_a   = rein_cand.i_a
	       AND    ints_cand.tail_node = rein_cand.tail_node
	       AND    dth_cand.tail_node  = ints_cand.tail_node
	       GROUP BY rein_cand.head_node) AS uhi_cand
        WHERE  rein_cand.head_node = uhi_cand.head_node
	AND    rein_cand.dth_t = uhi_cand.min_dth_t
	ORDER BY rein_cand.head_node;

	-- 4. drop the table of rein_cand
	DROP TABLE rein_cand;
END; $$
LANGUAGE 'plpgsql';

-- 7. determine transition between sequences
CREATE OR REPLACE FUNCTION ZR_Transition()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process
	SET	transition = filiation
	WHERE	filiation = 'annexa_d_obj'
	OR	filiation = 'annexa_s_obj' 
	OR	filiation = 'merging' 
	OR	filiation = 'separa_s_obj' 
	OR	filiation = 'separa_d_obj' 
	OR	filiation = 'splitting'
	OR	filiation = 'disappearance'
	OR	filiation = 'appearance';

	CREATE TABLE trans_stat AS
	SELECT	DISTINCT obj_id, pro_seq_id, filiation,
		MIN(t_s) OVER (PARTITION BY obj_id, pro_seq_id) AS min_t_s, 
		MAX(t_s) OVER (PARTITION BY obj_id, pro_seq_id) AS max_t_s
	FROM	uhi_process
	WHERE	transition IS NULL
	ORDER BY obj_id, pro_seq_id, min_t_s;

	CREATE TABLE trans_temp AS
	SELECT	cur_pre.obj_id, cur_pre.cur_pro_seq_id, cur_pre.pre_fil, cur_pre.cur_fil, cur_nex.nex_fil
	FROM   (SELECT	cur.obj_id,
			cur.pro_seq_id AS cur_pro_seq_id,
			cur.filiation AS cur_fil,
			pre.filiation AS pre_fil
		FROM   (SELECT	obj_id, pro_seq_id, min_t_s, filiation
			FROM	trans_stat) AS cur,
		       (SELECT	obj_id, max_t_s, filiation
			FROM	trans_stat) AS pre
		WHERE	cur.obj_id = pre.obj_id
		AND	cur.min_t_s - pre.max_t_s = '01:00:00'::INTERVAL
		ORDER BY obj_id, pro_seq_id) AS cur_pre
	LEFT JOIN
	       (SELECT	cur.obj_id,
			cur.pro_seq_id AS cur_pro_seq_id,
			cur.filiation AS cur_fil,
			nex.filiation AS nex_fil
		FROM   (SELECT	obj_id, pro_seq_id, max_t_s, filiation
			FROM	trans_stat) AS cur,
		       (SELECT	obj_id, min_t_s, filiation
			FROM	trans_stat) AS nex
		WHERE	cur.obj_id = nex.obj_id
		AND	nex.min_t_s - cur.max_t_s = '01:00:00'::INTERVAL
		ORDER BY obj_id, pro_seq_id) AS cur_nex
	ON	cur_pre.obj_id = cur_nex.obj_id
	AND	cur_pre.cur_pro_seq_id = cur_nex.cur_pro_seq_id
	AND	cur_pre.cur_fil = cur_nex.cur_fil
	ORDER BY obj_id, cur_pro_seq_id;

	-- 1
	PERFORM ZR_Plateau();
	PERFORM ZR_Plateau_Expan2Conti();
	PERFORM ZR_Plateau_Conti2Contr();
	PERFORM ZR_Floor();
	PERFORM ZR_Floor_Contr2Conti();
	PERFORM ZR_Floor_Conti2Expan();
	PERFORM ZR_Stabilization();
	PERFORM ZR_Resumption();
	PERFORM ZR_Peak();
	PERFORM ZR_Low();
	
	DROP TABLE trans_temp;
	DROP TABLE trans_stat;
END; $$
LANGUAGE 'plpgsql';

-- 8. 
CREATE OR REPLACE FUNCTION ZR_Plateau()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'plateau'
	FROM	trans_temp AS t
	WHERE	t.cur_pro_seq_id = p.pro_seq_id
	AND	t.pre_fil = 'expansion'
	AND	t.cur_fil = 'continuation'
	AND	t.nex_fil = 'contraction';	
	
	UPDATE	uhi_process AS p
	SET	transition = 'reach_plateau'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'plateau') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'leave_plateau'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s + '01:00:00'::INTERVAL) AS nex_t_s
			FROM	uhi_process
			WHERE transition = 'plateau') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.nex_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 9. 
CREATE OR REPLACE FUNCTION ZR_Plateau_Expan2Conti()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'plateau'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'expasion'
		AND	cur_fil = 'continuation') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'reach_plateau'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'plateau') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 10. 
CREATE OR REPLACE FUNCTION ZR_Plateau_Conti2Contr()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'leave_plateau'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'continuation'
		AND	cur_fil = 'contraction') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'plateau'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'leave_plateau') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 11. 	
CREATE OR REPLACE FUNCTION ZR_Floor()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'floor'
	FROM	trans_temp AS t
	WHERE	t.cur_pro_seq_id = p.pro_seq_id
	AND	t.pre_fil = 'contraction'
	AND	t.cur_fil = 'continuation'
	AND	t.nex_fil = 'expansion';

	UPDATE	uhi_process AS p
	SET	transition = 'reach_floor'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'floor') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'leave_floor'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s + '01:00:00'::INTERVAL) AS nex_t_s
			FROM	uhi_process
			WHERE transition = 'floor') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.nex_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 12. 
CREATE OR REPLACE FUNCTION ZR_Floor_Contr2Conti()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'floor'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'contraction'
		AND	cur_fil = 'continuation') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'reach_floor'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'floor') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 13. 
CREATE OR REPLACE FUNCTION ZR_Floor_Conti2Expan()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'leave_floor'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'continuation'
		AND	cur_fil = 'expansion') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;

	UPDATE	uhi_process AS p
	SET	transition = 'floor'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id, (t_s - '01:00:00'::INTERVAL) AS pre_t_s
			FROM	uhi_process
			WHERE transition = 'leave_floor') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 14. 
CREATE OR REPLACE FUNCTION ZR_Stabilization()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'stabilization'
	FROM	trans_temp AS t
	WHERE	t.cur_pro_seq_id = p.pro_seq_id
	AND	t.pre_fil = 'contraction'
	AND	t.cur_fil = 'continuation'
	AND	t.nex_fil = 'contractioin';

	UPDATE	uhi_process AS p
	SET	transition = 'stabilization'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id,
				(t_s - '01:00:00'::INTERVAL) AS pre_t_s,
				(t_s + '01:00:00'::INTERVAL) AS nex_t_s
			FROM	uhi_process
			WHERE transition = 'stabilization') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s
		OR	p.obj_id = t.obj_id
		AND	p.t_s = t.nex_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 15. 	
CREATE OR REPLACE FUNCTION ZR_Resumption()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'resumption'
	FROM	trans_temp AS t
	WHERE	t.cur_pro_seq_id = p.pro_seq_id
	AND	t.pre_fil = 'expansion'
	AND	t.cur_fil = 'continuation'
	AND	t.nex_fil = 'expansion';

	UPDATE	uhi_process AS p
	SET	transition = 'resumption'
	FROM   (SELECT	p.obj_id, p.pro_seq_id
		FROM	uhi_process AS p,	
		       (SELECT	obj_id,
				(t_s - '01:00:00'::INTERVAL) AS pre_t_s,
				(t_s + '01:00:00'::INTERVAL) AS nex_t_s
			FROM	uhi_process
			WHERE transition = 'resumption') AS t
		WHERE	p.obj_id = t.obj_id
		AND	p.t_s = t.pre_t_s
		OR	p.obj_id = t.obj_id
		AND	p.t_s = t.nex_t_s) AS candid
	WHERE	p.obj_id = candid.obj_id
	AND	p.pro_seq_id = candid.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 16. 	
CREATE OR REPLACE FUNCTION ZR_Peak()
RETURNS VOID AS $$
BEGIN	
	UPDATE	uhi_process AS p
	SET	transition = 'peak'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'expasion'
		AND	cur_fil = 'contraction') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';

-- 17. 
CREATE OR REPLACE FUNCTION ZR_Low()
RETURNS VOID AS $$
BEGIN
	UPDATE	uhi_process AS p
	SET	transition = 'low'
	FROM   (SELECT	*
		FROM	trans_temp
		WHERE	nex_fil IS NULL
		AND	pre_fil = 'contraction'
		AND	cur_fil = 'expasion') AS t
	WHERE	p.transition IS NULL
	AND	t.obj_id = p.obj_id
	AND	t.cur_pro_seq_id = p.pro_seq_id;
END; $$
LANGUAGE 'plpgsql';
	
-- 8. to list all the process and sequence for filiation
SELECT ZR_Process();
SELECT ZR_Sequence();
SELECT ZR_Transition();
 
SELECT * FROM uhi_process ORDER BY obj_id, pro_seq_id, t_s;


-- 9. to list all the reincarnated UHIs (This calculation is heavy)
SELECT ZR_Reincarnate(168, 0.53, 0.5001);