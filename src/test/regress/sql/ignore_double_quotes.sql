DROP SCHEMA sch_ignore_quote_01 CASCADE;
CREATE SCHEMA sch_ignore_quote_01;
SET CURRENT_SCHEMA TO sch_ignore_quote_01;

-- test tables with the same name but different case already exists in the database
-- table
create table "TeSt" ("A" int);
select * from test;
select * from "TeSt";
create table test ("A" int, "a" int);
insert into test("A","a") values(1,2);
select * from test;

-- view
CREATE VIEW test_view AS SELECT * FROM test;
CREATE VIEW "TeSt_view" AS SELECT * FROM "TeSt";
select * from test_view;
select * from "TeSt_view";

-- materialized view
CREATE MATERIALIZED VIEW m_test_view AS SELECT * FROM test;
CREATE MATERIALIZED VIEW "M_teSt_view" AS SELECT * FROM "TeSt";
select * from m_test_view;
select * from "M_teSt_view";

-- index
CREATE INDEX i1 ON test USING btree(a);
CREATE INDEX "I1" ON "TeSt" USING btree("A");
drop index "I1";

-- Chinese characters
create table "￥" ("￥" int, "$" int);
create table "啊啊" ("，" int, "," int);
select * from "￥";
select * from ￥;
select * from "$"; -- errors
select * from "啊啊";
insert into ￥("￥","$") values(1,2);
insert into ￥(￥,"$") values(4,3);
insert into "啊啊"("，",",") values(1,2);

-- function
create function f1(b int) returns int
as $$
begin
    return b;
end;
$$language plpgsql;

create function "F1"(b int) returns int
as $$
begin
    b=b+1;
	  return b;
end;
$$language plpgsql;

call f1(8);
call "F1"(8);

-- procedure
create procedure p1() is
begin
    insert into test("A","a") values(5,9);
end;
/

create procedure "P1"() is
begin
	  insert into "TeSt"("A") values(10);
end;
/

call p1();
select * from test;
call "P1"();
select * from "TeSt";

-- type
CREATE TYPE compfoo AS (f1 int, f2 text);
CREATE TYPE "CompFoo" AS (f3 text, f4 int, f5 int);
CREATE TABLE t1_compfoo(a int, b compfoo);
CREATE TABLE t2_compfoo(a int, b "CompFoo");
INSERT INTO t1_compfoo values(1,(1,'demo1'));
INSERT INTO t2_compfoo values(2,('demo2', 3, 5));
select * from t1_compfoo;
select * from t2_compfoo;

-- sequence
CREATE SEQUENCE s1 START 101 CACHE 20;
CREATE SEQUENCE "S1" START 801 CACHE 90;
drop sequence "S1";

-- test enable_ignore_case_in_dquotes=on
set enable_ignore_case_in_dquotes=on;

create table test1 ("A" int, 'a' int);-- error
insert into test("A","a") values(2,3);-- error
select * from test;
select * from test_view;
select * from "TeSt_view";-- lowcase
select * from m_test_view;
select * from "M_teSt_view";-- lowcase
call f1(8);
call "F1"(8); -- lowcase
call p1(8);
call "P1"(8); -- lowcase
CREATE TABLE t3_compfoo(a int, b "CompFoo");
CREATE INDEX "I1" ON "TeSt" USING btree("A");
INSERT INTO t3_compfoo values(2,('demo2', 3, 5));-- error
INSERT INTO t3_compfoo values(1,(1,'demo1'));
select * from t3_compfoo;
CREATE SEQUENCE "S1" START 801 CACHE 90;-- error
call p1();
select * from test;
call "P1"();
select * from "TeSt";
create table "SCH_ignore_quote_01"."TAB_quote"("A" int);
insert into tab_quote (a) values (4);
insert into "SCH_IGNORE_QUOTE_01"."TAB_QUOTE" ("A") values (5);
select a from tab_quote;
select t.a from sch_ignore_quote_01.tab_quote t;
select t."A" from "SCH_IGNORE_QUOTE_01"."TAB_QUOTE" t;
create table "￥￥" ("￥" int, "$" int);
create table "$" ("，" int, "," int);
select * from "￥";
select * from ￥;
select * from "$";
select * from "啊啊";
insert into ￥("￥","$") values(5,6);
insert into ￥(￥,"$") values(7,8);
insert into "啊啊"("，",",") values(10,11);

-- clean
drop table TAB_quote;
set enable_ignore_case_in_dquotes=off;
drop materialized view m_test_view;
drop materialized view "M_teSt_view";
drop index i1;
drop view test_view;
drop view "TeSt_view";
drop table test;
drop table "TeSt";
drop table "￥￥";
drop table "$";
drop table ￥;
drop table "啊啊";
drop function f1;
drop function "F1";
drop procedure p1;
drop procedure "P1";
drop table t1_compfoo;
drop table t2_compfoo;
drop table t3_compfoo;
drop type compfoo;
drop type "CompFoo";
drop sequence s1;
drop schema sch_ignore_quote_01;
