drop schema if exists plpgsql_recompile cascade;
create schema plpgsql_recompile;
set current_schema = plpgsql_recompile;
set behavior_compat_options = 'plpgsql_dependency';
---test 1
create type s_type as (
    id integer,
    name varchar,
    addr text
);

create or replace procedure type_alter(a s_type)
is
begin    
    RAISE INFO 'call a: %', a;
end;
/
select valid from pg_object where object_type='P' and object_oid 
in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = 
(select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai'));

alter function type_alter compile;
alter procedure type_alter(s_type) compile;

alter type s_type ADD attribute a int;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai',100));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

alter type s_type DROP attribute a;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1,'zhang','shanghai'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

alter type s_type ALTER attribute id type float;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
alter procedure type_alter compile;
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));
call type_alter((1.0,'zhang','shanghai'));
select valid from pg_object where object_type='P' and object_oid in (select Oid from pg_proc where propackageid = 0 and proname='type_alter' and pronamespace = (select Oid from pg_namespace where nspname = 'plpgsql_recompile'));

-- report error
alter type s_type RENAME to s_type_rename;
ALTER type s_type RENAME ATTRIBUTE name TO new_name;
call type_alter((1,'zhang','shanghai'));

-- test 2
drop table if exists stu;
create table stu(sno int, name varchar, sex varchar, cno int);
create type r1 as (a int, c stu%RowType);

drop package if exists pkg;
create or replace package pkg
is    
procedure proc1(p_in r1);
end pkg;
/
create or replace package body pkg
is
declare
v1 r1;
v2 stu%RowType;
procedure proc1(p_in r1) as
begin        
RAISE INFO 'call p_in: %', p_in;
end;
end pkg;
/

call pkg.proc1((1,(1,'zhang','M',1)));
alter package pkg compile;
alter package pkg compile specification;
alter package pkg compile body;
alter package pkg compile package;

alter table stu ADD column b int;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1,2)));


alter table stu DROP column b;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1)));

alter table stu RENAME to stu_re;
alter package pkg compile;
call pkg.proc1((1,(1,'zhang','M',1)));

alter type r1 ALTER attribute a type float;
alter package pkg compile;
call pkg.proc1((1.0,(1,'zhang','M',1)));

--compile error
create or replace package body pkg
is
declare
v1 r1%RowType;
procedure proc1(p_in r1) as
begin
select into v1 values (1,ROW(1,'zhang','M',1));     
RAISE INFO 'call p_in: %', p_in;
end;
end pkg;
/
alter package pkg compile;

-- select oid,* from pg_type where typname='stu';
drop schema plpgsql_recompile cascade;
reset behavior_compat_options;