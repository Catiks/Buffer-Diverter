create user bot identified by 'Gauss@123' profile default; 
alter user bot sysadmin; 
create database tpcc1000 encoding 'UTF8' template=template0 owner bot;

create user tom identified by 'Gauss@123' profile default;
alter user tom sysadmin;
create database tpcc1000_tom encoding 'UTF8' template=template0 owner tom;


