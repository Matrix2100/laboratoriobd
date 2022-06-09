alter session set "_ORACLE_SCRIPT"=true;  

create tablespace dados_acad
  logging
  datafile 'c:\oraclexe\dados\dados_acad.dbf' 
  size 500m 
  autoextend on 
  next 100m maxsize 3072m
  extent management local;
  
create tablespace indices_acad
  logging
  datafile 'c:\oraclexe\dados\indices_acad.dbf' 
  size 200m 
  autoextend on 
  next 10m maxsize 1024m
  extent management local;  

create temporary tablespace temp_acad 
  tempfile 'c:\oraclexe\dados\temp_acad.dbf' 
  size 50m 
  autoextend on 
  next 10m maxsize 200m
  extent management local;

create user ACADEMICO
  identified by ACADEMICO
  default tablespace DADOS_ACAD
  temporary tablespace TEMP_ACAD
  profile DEFAULT;

grant connect to ACADEMICO;
grant resource to ACADEMICO;
grant create view to ACADEMICO;
ALTER USER ACADEMICO quota unlimited on DADOS_ACAD;
ALTER USER ACADEMICO quota unlimited on INDICES_ACAD;

create user TABELAEES identified by TABELAEES;
grant all privileges to TABELAEES;