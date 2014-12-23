# Database URN Manager

This URN manager stores the URN <> URL mappings in a database.

The format of the generated URN: `urn:<domain>:yyyy/MM/dd-<uuid>`

## Partitioning

The table has been designed to be partitioned on the "created" field which has a day-level granularity.
Obviously this is only useful if we have the date when looking up a mapping. For this reason the manager will generate the date into the URN. This has the additional benefit that it immediately makes it clear how old the data linked to the URN is approximately.

The UUID is also the primary key of the table but seldom used as this is a mandatory global index instead of a local index.

Instead lookups are performed using a combination of the created date (to pinpoint the partition) and the urn which should have a local index.

## Domain

The domain should be a globally unique identifier for your context. For example "com.mycompany".
It could be interesting to make it more specific like "com.mycompany.eai" to differentiate it from other departments.

Additionally it is interesting to add the environment to the URNs, this allows you to recognize easily from which environment a URN is and allows (if you want) automatic lookup of the data using a remote connector to the appropriate environment.

E.g. for development it could be "com.mycompany.eai.dev", quality would be "com.mycompany.eai.qlty" and prd would simply be "com.mycompany.eai".