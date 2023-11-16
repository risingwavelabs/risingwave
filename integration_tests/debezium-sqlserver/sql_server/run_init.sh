/opt/mssql-tools/bin/sqlcmd -S sqlserver -U sa -P YourPassword123 -d master -Q 'create database mydb;'
/opt/mssql-tools/bin/sqlcmd -S sqlserver -U sa -P YourPassword123 -d master -i /init.sql
