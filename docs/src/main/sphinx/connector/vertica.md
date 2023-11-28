---
myst:
  substitutions:
    default_domain_compaction_threshold: '`32`'
---

# Vertica connector

```{raw} html
<img src="../_static/img/druid.png" class="connector-logo">
```

The Vertica connector allows querying and creating tables 
in an external [Vertica](https://www.vertica.com/) database.
This can be used to join data between different systems 
like Vertica and PostgresSQL, or between different Vertica instances.

## Requirements

To connect to Vertica, you need:

- Vertica 23.4.x or higher.
- Network access from the Trino coordinator and workers to Vertica.
  Port 5433 is the default port.

## Configuration

The connector can query a database on a Vertica server. Create a catalog 
properties file that specifies the Vertica connector by setting 
the `connector.name` to `Vertica`.

For example, to access a database as the `example` catalog, create the file
`etc/catalog/example.properties`. Replace the connection properties as
appropriate for your setup:

```text
Connector.Name=vertica  
connection-url=jdbc:vertica://vertica.Example.Com:5433/database  
connection-user=myuser  
connection-password=mysecretpassword
```

The `connection-url` defines the connection information and parameters to pass
to the Vertica JDBC driver. The parameters for the URL are available in the
[Vertica JDBC driver documentation](https://docs.vertica.com/23.4.x/en/connecting-to/client-libraries/accessing/java/creating-and-configuring-connection/).
Some parameters can have adverse effects on the connector behavior or not work
with the connector.

The `connection-user` and `connection-password` are typically required and
determine the user credentials for the connection, often a service user. You can
use {doc}`secrets </security/secrets>` to avoid actual values in the catalog
properties files.

### Access to system tables



The Vertica connector supports reading [Vertica system tables](https://docs.vertica.com/23.4.x/en/sql-reference/system-tables/). 
The functionality is turned off by default, and can be enabled 
using the `vertica.include-system-tables` configuration property.

You can see more details in the  `system_tables` in the `example` catalog,
for example about the `system_tables` system table:

```sql
SELECT * FROM system_tables ORDER BY table_schema, table_name;
```

> **_NOTE :_** System table names and schemas may vary in different Vertica installations,
> so replace `system_table` with the specific system table you want to query in 
> your Vertica database. 
> Ensure you have the necessary privileges to access system tables and exercise caution 
> when querying these tables, as they contain critical information about the Vertica system.


### Connection security

If you have TLS configured with a globally-trusted certificate installed on your
data source, you can enable TLS between your cluster and the data
source by appending a parameter to the JDBC connection string set in the
`connection-url` catalog configuration property.

For example, with version 42 of the Vertica JDBC driver, enable TLS by
appending the `ssl=true` parameter to the `connection-url` configuration
property:

```properties
connection-url=jdbc:vertica://vertica.Example.Com:5433/database?ssl=true 
```

For more information on TLS configuration options check [Vertica JDBC driver documentation](https://docs.vertica.com/23.4.x/en/connecting-to/client-libraries/accessing/java/creating-and-configuring-connection/),
and to enable tls in vertica check [Vertica TLS documentation](https://docs.vertica.com/23.4.x/en/security-and-authentication/tls-protocol/tls-overview/tls-configs/).



### Multiple Vertica databases or servers

The Vertica connector can only access a single database within a Vertica server.
If you have multiple Vertica databases,or want to connect to multiple Vertica servers, 
you must configure multiple instances of the Vertica connector.

To add another catalog, simply add another properties file to `etc/catalog`
with a different name, making sure it ends in `.properties`. For example,
if you name the property file `sales.properties`, Trino creates a
catalog named `sales` using the configured connector.



## Type mapping

Trino and Vertica each support types that the other does not, this connector 
[modifies some types](https://trino.io/docs/current/language/types.html#type-mapping-overview) when reading or writing data. Data types may not map 
the same way in both directions between Trino and the Vertica data source. 
Refer to the following sections for type mapping in each direction.

### Vertica type to Trino type mapping

The connector maps Vertica types to the corresponding Trino types following
this table:

:::{list-table} Vertica type to Trino type mapping
:widths: 30, 30, 40
:header-rows: 1

  * - Vertica type
    - Trino type
    - Notes
  * - ``BINARY``
    - ``VARBINARY``
    -
  * - ``VARBINARY (BYTEA, RAW)``
    - ``VARBINARY``
    -
  * - ``LONG VARBINARY``
    - ``VARBINARY``
    -
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``CHAR``
    - ``CHAR``
    -
  * - ``VARCHAR``
    - ``VARCHAR``
    -
  * - ``LONG VARCHAR``
    - ``VARCHAR``
    -
  * - ``DATE``
    - ``DATE``
    - 
  * - ``TIME``
    - ``TIME``
    -
  * - ``TIME WITH TIMEZONE``
    - ``TIME WITH TIMEZONE``
    -
  * - ``TIMESTAMP (DATETIME, SMALLDATETIME)``
    - ``TIMESTAMP``
    -
  * - ``TIMESTAMP WITH TIMEZONE``
    - ``TIMESTAMP WITH TIMEZONE``
    -
  * - ``INTERVAL``
    - ``INTERVAL``
    -
  * - ``INTERVAL DAY TO SECOND``
    - ``INTERVAL``
    -
  * - ``INTERVAL YEAR TO MONTH``
    - ``INTERVAL``
    -
  * - ``DOUBLE PRECISION``
    - ``DOUBLE``
    -
  * - ``FLOAT``
    - ``REAL``
    -
  * - ``FLOAT(n)``
    - ``REAL``
    -
  * - ``FLOAT8``
    - ``REAL``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``INT``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``INT8``
    - ``BIGINT``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``TINYINT``
    - ``SMALLINT``
    -
  * - ``DECIMAL``
    - ``DECIMAL``
    -
  * - ``NUMERIC``
    - ``DECIMAL``
    -
  * - ``NUMBER``
    - ``DECIMAL``
    -
  * - ``UUID``
    - ``UUID``
    -
  * - ``GEOMETRY``
    - ``VARBINARY``
    - Dependent on the use case.
  * - ``GEOGRAPHY``
    - ``VARBINARY``
    - Dependent on the use case.
  * - ``ARRAY``
    - Disabled,``ARRAY``
    - See [](vertica-array-type-handling) for more information.
  
:::

No other types are supported.


### Trino type to Vertica type mapping

The connector maps Trino types to the corresponding Vertica types following
this table:

:::{list-table} Trino type to Vertica type mapping
:widths: 30, 30, 40
:header-rows: 1

  * - Trino type
    - Vertica type
    - Notes
  * - ``BOOLEAN``
    - ``BOOLEAN``
    -
  * - ``SMALLINT``
    - ``SMALLINT``
    -
  * - ``TINYINT``
    - ``SMALLINT``
    -
  * - ``INTEGER``
    - ``INTEGER``
    -
  * - ``BIGINT``
    - ``BIGINT``
    -
  * - ``DOUBLE``
    - ``DOUBLE``
    -
  * - ``DECIMAL(p, s)``
    - ``NUMERIC(p, s)``
    - ``DECIMAL(p, s)`` is an alias of  ``NUMERIC(p, s)``
    -
  * - ``CHAR(n)``
    - ``CHAR(n)``
    -
  * - ``VARCHAR(n)``
    - ``VARCHAR(n)``
    -
  * - ``VARBINARY``
    - ``VARBINARY``
    -
  * - ``DATE``
    - ``DATE``
    -
  * - ``TIME(n)``
    - ``TIME(n)``
    -
  * - ``TIMESTAMP(n)``
    - ``TIMESTAMP(n)``
    -
  * - ``TIMESTAMP(n) WITH TIME ZONE``
    - ``TIMESTAMPTZ(n)``
    -
  * - ``UUID``
    - ``UUID``
    -
  * - ``JSON``
    - ``JSONB``
    -
  * - ``ARRAY``
    - ``ARRAY``
    - See [](vertica-array-type-handling) for more information.
:::

No other types are supported.

 ``` 
 Note: Mapping may depend on the actual data and queries you are working with. 
       Array type handling may vary depending on the specific use case. 
 ```



(vertica-array-type-handling)=

### Array type handling

The Vertica array implementation differs from Trino in that it does not support fixed dimensions, whereas Trino supports only arrays with fixed dimensions. You can configure how the `Vertica` connector handles arrays with the `vertica.array-mapping` configuration property in your catalog file or the `array_mapping` session property. 

The following values are accepted for this property:

- `DISABLED` (default): array columns are skipped.
- `AS_ARRAY`: array columns are interpreted as Trino `ARRAY` type, for array columns without fixed dimensions.


```{include} jdbc-type-mapping.fragment
```

## Querying Vertica

The Vertica connector provides a schema for every Vertica schema. 
You can see the available Vertica schemas by running `SHOW SCHEMAS`: 

```
SHOW SCHEMAS FROM example; 
```

If you have a Vertica schema named `public`, you can view the tables 
in this schema by running `SHOW TABLES`: 

```
SHOW TABLES FROM example.public;
```

You can see a list of the columns in the `customer_dimension` table in the `public` database
using either of the following:

```
DESCRIBE example.public.customer_dimension;
SHOW COLUMNS FROM example.public.customer_dimension;
```

Finally, you can access the `customer_dimension` table in the `public` schema:

```
SELECT * FROM example.public.customer_dimension;
```

If you used a different name for your catalog properties file, use
that catalog name instead of `example` in the above examples.

These queries allow you to explore and interact with your Vertica 
data using Trino in a similar manner as querying Vertica . 

(Vertica-sql-support)=

## SQL support

The connector provides read access and write access to data and metadata in
Vertica.  In addition to the {ref}`globally available
<sql-globally-available>` and {ref}`read operation <sql-read-operations>`
statements, the connector supports the following features:

- {doc}`/sql/insert`
- {doc}`/sql/update`
- {doc}`/sql/delete`
- {doc}`/sql/truncate`
- {ref}`sql-schema-table-management`

```{include} sql-update-limitation.fragment
```

```{include} sql-delete-limitation.fragment
```

```{include} alter-table-limitation.fragment
```

```{include} alter-schema-limitation.fragment
```

(vertica-fte-support)=

## Fault-tolerant execution support

The connector supports {doc}`/admin/fault-tolerant-execution` of query
processing. Read and write operations are both supported with any retry policy.

## Table functions

The connector provides specific {doc}`table functions </functions/table>` to
access Vertica.

(vertica-query-function)=

### `query(varchar) -> table`

The `query` function allows you to query the underlying database directly. It
requires syntax native to Vertica, because the full query is pushed down and
processed in Vertica. This can be useful for accessing native features which
are not available in Trino or for improving query performance in situations
where running a query natively may be faster.

The native query passed to the underlying data source is required to return 
a table as a result set. Only the data source performs validation or security 
checks for these queries using its own configuration. Trino does not perform 
these tasks. Only use passthrough queries to read data. 

```{include} query-passthrough-warning.fragment
```

As a simple example, query the `example` catalog and select an entire table:

```
SELECT
  *
FROM
  TABLE(
    example.system.query(
      query => 'SELECT
        *
      FROM
        public.customer_dimension'
    )
  );
```


```{include} query-table-function-ordering.fragment
```

## Performance

The connector includes a number of performance improvements, detailed in the
following sections.

(vertica-table-statistics)=

### Table statistics for Vertica

The Vertica connector can use {doc}`table and column statistics
</optimizer/statistics>` for {doc}`cost based optimizations
</optimizer/cost-based-optimizations>`, to improve query processing performance
based on the actual data in the data source.


#### Note: Show stats works only when you collects stats first on vertica use the command :

``` 
SELECT ANALYZE_STATISTICS (''); 
```

For more details refer [Vertica Documentation ](https://docs.vertica.com/23.4.x/en/admin/collecting-db-statistics/) .


The statistics are collected by Vertica and retrieved by the connector.

To collect statistics for a table, execute the following statement in
Vertica.

```text
SHOW stats catalog.table_schema.table_name; 
```

Refer to the Vertica documentation for additional ANALYZE options and 
details on collecting statistics for your tables. 

By collecting and providing accurate statistics, you can help the Vertica connector 
make more informed decisions about query optimization and execution plans, leading to improved query performance. 

(vertica-pushdown)=

### Pushdown

The connector supports pushdown for a number of operations:

- {ref}`join-pushdown`
- {ref}`limit-pushdown`
- {ref}`topn-pushdown`

{ref}`Aggregate pushdown <aggregation-pushdown>` for the following functions:

- {func}`count`
- {func}`max`
- {func}`min`
- {func}`sum`
  

```{include} pushdown-correctness-behavior.fragment
```

```{include} join-pushdown-enabled-true.fragment
```

### Predicate pushdown support

Predicates are pushed down for most types, including `UUID` and temporal
types, such as `DATE`.

The connector does not support pushdown of range predicates, such as `>`,
`<`, or `BETWEEN`, on columns with {ref}`character string types
<string-data-types>` like `CHAR` or `VARCHAR`.  Equality predicates, such as
`IN` or `=`, and inequality predicates, such as `!=` on columns with
textual types are pushed down. This ensures correctness of results since the
remote data source may sort strings differently than Trino.

In the following example, the predicate of the first query is not pushed down
since `name` is a column of type `VARCHAR` and `>` is a range predicate.
The other queries are pushed down.

```sql
-- Not pushed down
SELECT * FROM nation WHERE name > 'CANADA';
-- Pushed down
SELECT * FROM nation WHERE name != 'CANADA';
SELECT * FROM nation WHERE name = 'CANADA';
```

There is experimental support to enable pushdown of range predicates on columns
with character string types which can be enabled by setting the
`vertica.experimental.enable-string-pushdown-with-collate` catalog
configuration property or the corresponding
`enable_string_pushdown_with_collate` session property to `true`.
Enabling this configuration will make the predicate of all the queries in the
above example get pushed down.