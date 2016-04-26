This tool can help generate subsets of SQL databases.

1. Edit tables.yaml to describe the tables that should be exported,
   and how (if at all) they should be shrunk, using SQL WHERE fragments.

2. Generate import and export SQL scripts:

 $ mkdir /tmp/smalldump
 $ stack exec -- postgres-subset --tables ./tables.yaml --directory=/tmp/smalldump/

3. On the exporting database:

 $ rm -fv /tmp/smalldump/* -vf

 # dump the schema
 $ pg_dump --format=custom --schema-only --dbname=beautilytics > /tmp/smalldump/schema.sql

 # dump a subset of the data
 $ psql beautilytics -f export.sql 

4. On the importing database:

 $ psql postgres -c "DROP DATABASE beautilytics"
 $ psql postgres -c "CREATE DATABASE beautilytics"
 $ pg_restore --schema-only --dbname=beautilytics --no-owner --no-acl

Note that the path specified by --directory is on the database server,
not on the client.

