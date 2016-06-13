This tool can help generate subsets of SQL databases.

1. Edit tables.yaml to describe the tables that should be exported,
   and how (if at all) they should be shrunk, using SQL WHERE fragments.

2. Create/clear out dump directory:

```
  $ rm -fv /tmp/smalldump/* -vf
```

3. Generate import and export SQL scripts:

```
  $ stack exec -- postgres-subset --tables ./tables.yaml --directory=/tmp/smalldump/ --db="host='db' user='benc' dbname='beautilytics'" 
```

3. On the exporting database:

```
  $ cd /tmp/smalldump
  # dump the schema
  $ pg_dump --format=custom --schema-only --dbname=beautilytics > schema.sql

 # dump a subset of the data
 $ psql --user=readonly beautilytics -f export.sql
```

4. Move the dump dir between machines, if desired.

5. On the importing database:

```
 $ cd dump
 $ psql postgres -c "DROP DATABASE beautilytics"
 $ psql postgres -c "CREATE DATABASE beautilytics"
 $ pg_restore --schema-only --dbname=beautilytics --no-owner --no-acl < schema.sql
 $ psql beautilytics < import.sql
```

Approximate timings are, on Ben's computer, approximately 4 minutes
for export and 4 minutes for import.

