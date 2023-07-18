-- point
create table pt_test (id integer, geom st_point);

insert into pt_test values (1, st_pointfromtext('point(2 2)',0));
insert into pt_test values (2, st_pointfromtext('point(5.777 5.777)',0));
insert into pt_test values (3, st_pointfromtext('point(3.755 8.555)',0));

-- linestring
create table ls_test (id integer, geom st_linestring);

insert into ls_test values (1, st_linefromtext('linestring(2.555 1, 7 4, 12 6, 13 2, 14 2, 14 3)',0));
insert into ls_test values (2, st_linefromtext('linestring(0.55 12, 3 15, 7 13, 7 9, 1 9)',0));
insert into ls_test values (3, st_linefromtext('linestring(7.555 2, 6 5, 8 9, 9 10, 5 11)',0));

-- polygon
create table pg_test (id integer, geom st_polygon);

insert into pg_test values (1, st_polyfromtext('polygon((1 10,2.555 10,2 11,1 11,1 10))',1));
insert into pg_test values (2, st_polyfromtext('polygon((1 7,4.555 7,4 9,1 7))',1));
insert into pg_test values (3, st_polyfromtext('polygon((1 1,3.555 1,3 3,1 3,1 1))',1));

-- multi point linestring polygon
create table sp_test (sid int, spoint ST_POINT, sline ST_LINESTRING, spoly ST_POLYGON,  smpoint ST_MULTIPOINT, smline ST_MULTILINESTRING, smpoly ST_MULTIPOLYGON);

insert into sp_test (sid, spoint, sline, spoly, smpoint, smline, smpoly)
values (
  1,
  ST_GeomFromText('POINT(10 10)'),
  ST_GeomFromText('LINESTRING(10 10, 20 25, 15 40)'),
  ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1))'),
  ST_GeomFromText('MULTIPOINT(10 10, 30 20)'),
  ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (20 15, 30 40))'),
  ST_GeomFromText('MULTIPOLYGON(((10 10, 10 20, 20 20, 20 10, 10 10)), ((40 40, 40 50, 50 50, 50 40, 40 40)))')
);

insert into sp_test (sid, spoint, sline, spoly, smpoint, smline, smpoly)
values (
  2,
  ST_GeomFromText('POINT(20 20)'),
  ST_GeomFromText('LINESTRING(15 15, 25 30, 20 45)'),
  ST_GeomFromText('POLYGON((0 3, 6 6, 9 3, 7 0, 3 0, 0 3))'),
  ST_GeomFromText('MULTIPOINT(20 20, 40 30)'),
  ST_GeomFromText('MULTILINESTRING((15 15, 25 25), (25 20, 35 45))'),
  ST_GeomFromText('MULTIPOLYGON(((20 20, 20 30, 30 30, 30 20, 20 20)), ((50 50, 50 60, 60 60, 60 50, 50 50)))')
);

insert into sp_test (sid, spoint, sline, spoly, smpoint, smline, smpoly)
values (
  3,
  ST_GeomFromText('POINT(30 30)'),
  ST_GeomFromText('LINESTRING(25 25, 35 40, 30 55, 25 25)'),
  ST_GeomFromText('POLYGON((2 7, 7 12, 12 12, 17 7, 12 2, 7 2, 2 7))'),
  ST_GeomFromText('MULTIPOINT(30 30, 50 40)'),
  ST_GeomFromText('MULTILINESTRING((25 25, 35 35), (35 30, 45 55))'),
  ST_GeomFromText('MULTIPOLYGON(((30 30, 30 40, 40 40, 40 30, 30 30)), ((60 60, 60 70, 70 70, 70 60, 60 60)))')
);