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

create table mo_test (mid int, mpoint ST_MPOINT, mline ST_MLINESTRING, mpoly ST_MPOLYGON);

insert into mo_test (mid, mpoint, mline, mpoly)
values (
  1,
  st_importfromwkt('MPOINT(2004/01/01
  12:00:01,1,1,2004/01/01 12:00:02,2,2,2004-01-01
  12:00:03,3,3,2004/01/01 12:00:04,4,4,2004-01-01
  12:00:05,5,5,2004/01/01 12:00:06,6,6,2004-01-01
  12:00:07,7,7,2004/01/01 12:00:08,8,8,2004-01-01
  12:00:09,9,9,2004/01/01 12:00:10,10,10)'),
  st_importfromwkt('MLINESTRING((2004/01/01 12:00:01,1,1,1,6,6,4,6,6,9,8))'),
  st_importfromwkt('MPOLYGON((2004/01/01 12:00:02,((3,3,7,3,7,7,3,7,3,3))), (2004/01/01 12:00:06,((3,3,7,3,7,7,3,7,3,3))))')
);

insert into mo_test (mid, mpoint, mline, mpoly)
values (
  2,
  st_importfromwkt('MPOINT(2004/01/01
  12:00:01,10,1,2004/01/01 12:00:02,9,2,2004/01/01
  12:00:03,8,3,2004/01/01 12:00:04,7,4,2004/01/01
  12:00:05,6,5,2004/01/01 12:00:06,5,6,2004/01/01
  12:00:07,4,7,2004/01/01 12:00:08,3,8,2004/01/01
  12:00:09,2,9,2004/01/01 12:00:10,1,10)'),
  st_importfromwkt('MLINESTRING((2004/01/01 12:00:04,2,2,3,3,3,2,2,2,5,5,5))'),
  st_importfromwkt('MPOLYGON((2004/01/01 12:00:00,((9,0,11,0,11,2,9,2,9,0))), (2004/01/01 12:00:06,((9,0,11,0,11,2,9,2,9,0))))')
);

insert into mo_test (mid, mpoint, mline, mpoly)
values (
  3,
  st_importfromwkt('MPOINT(2004/01/01
  12:00:01,5,1,2004/01/01 12:00:02,5,2,2004/01/01
  12:00:03,5,3,2004/01/01 12:00:04,5,4,2004/01/01
  12:00:05,5,5,2004/01/01 12:00:06,5,6,2004/01/01
  12:00:07,5,7,2004/01/01 12:00:08,5,8,2004/01/01
  12:00:09,5,9,2004/01/01 12:00:10,5,10)'),
  st_importfromwkt('MLINESTRING((2004/01/01 12:00:08,3,3,4,4,4,7,7,7,6,6,6,8,8,8))'),
  st_importfromwkt('MPOLYGON((2004/01/01 12:00:03,((6,5,8,5,8,7,6,7,6,5))), (2004/01/01 12:00:05,((6,5,8,5,8,7,6,7,6,5))))')
);