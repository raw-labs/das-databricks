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

An example query using our `das_workspace` catalog and `default` schema:
```sql
SELECT * FROM databricks.lineitem
WHERE l_shipmode IN ('AIR', 'REG AIR')
ORDER BY l_shipdate
LIMIT 10;
```
