# To publish the server as a docker image
```
sbt docker/Docker/publishLocal
```

# To run locally
```
sbt docker/run
```

```sql
DROP SERVER databricks CASCADE;
CREATE SERVER databricks FOREIGN DATA WRAPPER multicorn OPTIONS (
  wrapper 'multicorn_das.DASFdw',
  das_url 'localhost:50051',
  das_type 'databricks',
  host '<HOST>.cloud.databricks.com',
  token '...',
  catalog '...',
  schema '...',
  warehouse '...' -- pick a warehouse ID, found in the Databricks UI
);
DROP SCHEMA databricks CASCADE;                                                                                              
CREATE SCHEMA databricks;
IMPORT FOREIGN SCHEMA "default" FROM SERVER databricks INTO databricks;
```

Tables belonging to the catalog/schema are then exposed under the name `databricks`.
```sql
SELECT * FROM databricks.my_table
WHERE ...
ORDER BY ...
LIMIT 10;
```
