brew install postgresql
pg_ctl -D /usr/local/var/postgres start

psql postgres
psql postgis_test -U meirkhan

export PGDATA='/usr/local/var/postgres'
pg_ctl status
brew services restart postgresql

CREATE USER postgres SUPERUSER;

----
select id, st_astext(location)
from buildings
where id = 3802522;
3802522 300972  "0"  

UPDATE buildings
SET location=ST_MakeValid(location);

------- To store geometry object
create table b (
	id integer,
	geom geometry
);

INSERT INTO b (id, geom )
  VALUES (1, ST_GeomFromText('POINT(-126.4 45.32)', 4326));
  
 select * from b;

 SELECT UpdateGeometrySRID('buildings','geometry',4326);


 ------ faster batch inserting
 https://stackoverflow.com/questions/8134602/psycopg2-insert-multiple-rows-with-one-query
         for row in reader:
            tup = (str(row[0]), "ST_GeomFromText(%s, %s)" % (row[1], int(row[2])))
            l.append(tup)

        args_str = ','.join(cursor.mogrify("(%s,%s)", x) for x in l)
        cursor.execute("INSERT INTO buildings VALUES " + args_str)


------------------ 1st question
::geography and ::geometry calculates different results, so if several trees located close enough, they can give different IDs

WITH 
cte as (
        SELECT 
          distinct on (b.id)  b.id building_id,
          t.id tree_id
        FROM b_test as b,
             t_test as t
        ORDER BY b.id, 
                 St_distance(b.location::geometry, t.location::geometry)
       )
SELECT 
  cte.building_id,
  cte.tree_id,
  St_distance(b.location, t.location)
FROM cte, 
     b_test b, 
     t_test t
WHERE b.id = cte.building_id
  AND t.id = cte.tree_id


-- to test
select 
	b.id as building_id,
	t.id as tree_id,
	st_distance(b.location, t.location),
	ROW_NUMBER() OVER(PARTITION BY b.id) rn
FROM b_test as b,
	   t_test as t
ORDER BY b.id, st_distance;



-------------------- 2 nd question

select b.id, (case when x.cnt is null then 0 else x.cnt end) cnt
from buildings1 b left join
(
select b.id building_id, count(b.id) cnt from
buildings1 b, trees1 t
WHERE st_dwithin(b.location, t.location, 100)
GROUP BY b.id) x
on b.id = x.building_id;


---Fastest
SELECT b.id, 
      (CASE WHEN x.cnt is NULL THEN 0 ELSE x.cnt END) cnt
FROM b_test b 
LEFT JOIN
      (
        SELECT b.id building_id, count(b.id) cnt 
        FROM b_test b, 
             t_test t
        WHERE st_dwithin(b.location::geography, t.location::geography, 100)
        GROUP BY b.id
      ) x
ON b.id = x.building_id;


-- final
WITH 
cte1 as (
        SELECT distinct on (b.id)  b.id building_id,
               t.id tree_id
        FROM buildings1 as b,
             trees1 as t
        ORDER BY b.id, St_distance(b.location::geometry, t.location::geometry)
        ),  
cte2 as (
        SELECT b.id, 
        (CASE WHEN en x.cnt is NULL THEN 0 ELSE x.cnt END) cnt
        FROM buildings1 b 
        LEFT JOIN (
                  SELECT b.id building_id, 
                         count(b.id) cnt 
                  FROM buildings1 b, trees1 t
                  WHERE st_dwithin(b.location::geography, t.location::geography, 100)
                  GROUP BY b.id
                  ) x
        ON b.id = x.building_id)
SELECT 
  cte1.building_id,
  cte1.tree_id,
  St_distance(b.location, t.location) dist,
  cte2.cnt
FROM cte1, 
     cte2,
     buildings1 b, 
     trees1 t
WHERE b.id = cte1.building_id
  and t.id = cte1.tree_id
  and cte1.building_id=cte2.id;


-- GEOMETRY vs GEOGRAPHY
https://medium.com/coord/postgis-performance-showdown-geometry-vs-geography-ec99967da4f0

-- St_distance doesn't use indexes
https://gis.stackexchange.com/questions/123911/st-distance-doesnt-use-index-for-spatial-query

-- PostGIS tricks
https://abelvm.github.io/sql/sql-tricks/

--- Cleaning
https://gis.stackexchange.com/questions/157091/cleaning-geometries-in-postgis/157325

-- copy
COPY joined TO '/Users/meirkhan/Desktop/output.csv' DELIMITER ',' CSV HEADER;

-- spped up st_Dwithin
https://gis.stackexchange.com/questions/221978/st-dwithin-calc-meters-transform-or-cast

-- eat elephant in small bites
http://dimensionaledge.com/intro-vector-tiling-map-reduce-postgis/

-- Postgis scalability overview
http://s3.cleverelephant.ca/2017-cdb-postgis.pdf

-- index
CREATE INDEX mytable_geom_x ON mytable USING GIST (geom);



