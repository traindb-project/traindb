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
