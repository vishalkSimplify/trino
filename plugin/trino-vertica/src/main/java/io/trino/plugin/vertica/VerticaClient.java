/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.vertica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.LongMath;
import com.google.inject.Inject;
import com.vertica.dsi.core.utilities.SqlType;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.plugin.base.aggregation.AggregateFunctionRewriter;
import io.trino.plugin.base.aggregation.AggregateFunctionRule;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcClient;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.BooleanReadFunction;
import io.trino.plugin.jdbc.ColumnMapping;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DoubleReadFunction;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcExpression;
import io.trino.plugin.jdbc.JdbcJoinCondition;
import io.trino.plugin.jdbc.JdbcSortItem;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTypeHandle;
import io.trino.plugin.jdbc.LongReadFunction;
import io.trino.plugin.jdbc.LongWriteFunction;
import io.trino.plugin.jdbc.ObjectReadFunction;
import io.trino.plugin.jdbc.ObjectWriteFunction;
import io.trino.plugin.jdbc.PredicatePushdownController;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.ReadFunction;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.plugin.jdbc.SliceReadFunction;
import io.trino.plugin.jdbc.SliceWriteFunction;
import io.trino.plugin.jdbc.UnsupportedTypeHandling;
import io.trino.plugin.jdbc.WriteMapping;
import io.trino.plugin.jdbc.aggregation.ImplementAvgFloatingPoint;
import io.trino.plugin.jdbc.aggregation.ImplementCorr;
import io.trino.plugin.jdbc.aggregation.ImplementCount;
import io.trino.plugin.jdbc.aggregation.ImplementCountAll;
import io.trino.plugin.jdbc.aggregation.ImplementCountDistinct;
import io.trino.plugin.jdbc.aggregation.ImplementCovariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementCovarianceSamp;
import io.trino.plugin.jdbc.aggregation.ImplementMinMax;
import io.trino.plugin.jdbc.aggregation.ImplementRegrIntercept;
import io.trino.plugin.jdbc.aggregation.ImplementRegrSlope;
import io.trino.plugin.jdbc.aggregation.ImplementStddevPop;
import io.trino.plugin.jdbc.aggregation.ImplementStddevSamp;
import io.trino.plugin.jdbc.aggregation.ImplementSum;
import io.trino.plugin.jdbc.aggregation.ImplementVariancePop;
import io.trino.plugin.jdbc.aggregation.ImplementVarianceSamp;
import io.trino.plugin.jdbc.expression.JdbcConnectorExpressionRewriterBuilder;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.expression.RewriteComparison;
import io.trino.plugin.jdbc.expression.RewriteIn;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.JoinCondition;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.JdbcJoinPushdownUtil.implementJoinCostAware;
import static io.trino.plugin.jdbc.JdbcMetadataSessionProperties.getDomainCompactionThreshold;
import static io.trino.plugin.jdbc.PredicatePushdownController.DISABLE_PUSHDOWN;
import static io.trino.plugin.jdbc.PredicatePushdownController.FULL_PUSHDOWN;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.fromTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharReadFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.trino.plugin.vertica.TypeUtils.getArrayElementTypeName;
import static io.trino.plugin.vertica.TypeUtils.getJdbcObjectArray;
import static io.trino.plugin.vertica.VerticaSqlConfig.ArrayMapping.AS_ARRAY;
import static io.trino.plugin.vertica.VerticaSqlConfig.ArrayMapping.DISABLED;
import static io.trino.plugin.vertica.VerticaSqlSessionProperties.getArrayMapping;
import static io.trino.plugin.vertica.VerticaSqlSessionProperties.isEnableStringPushdownWithCollate;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateTimeEncoding.unpackTimeNanos;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.Timestamps.MILLISECONDS_PER_SECOND;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.spi.type.Timestamps.round;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.UuidType.trinoUuidToJavaUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.RoundingMode.UNNECESSARY;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;

public class VerticaClient
        extends BaseJdbcClient
{
    private static final Logger log = Logger.get(VerticaClient.class);
    private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
    private static final int VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION = 6;
    private final Type uuidType;
    private final Type intervalDayToSec;
    private final Type intervalYearToMonth;
    private final List<String> tableTypes;
    private final boolean statisticsEnabled;
    private final ConnectorExpressionRewriter<ParameterizedExpression> connectorExpressionRewriter;
    private final AggregateFunctionRewriter<JdbcExpression, ?> aggregateFunctionRewriter;
    public static final long PICOSECONDS_PER_SECOND = 1_000_000_000_000L;
    public static final long PICOSECONDS_PER_MINUTE = PICOSECONDS_PER_SECOND * 60;
    public static final long PICOSECONDS_PER_HOUR = PICOSECONDS_PER_MINUTE * 60;
    public static final long PICOSECONDS_PER_DAY = PICOSECONDS_PER_HOUR * 24;

    private static final PredicatePushdownController VERTICA_STRING_COLLATION_AWARE_PUSHDOWN = (session, domain) -> {
        if (domain.isOnlyNull()) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        if (isEnableStringPushdownWithCollate(session)) {
            return FULL_PUSHDOWN.apply(session, domain);
        }

        Domain simplifiedDomain = domain.simplify(getDomainCompactionThreshold(session));
        if (!simplifiedDomain.getValues().isDiscreteSet()) {
            // Domain#simplify can turn a discrete set into a range predicate
            return DISABLE_PUSHDOWN.apply(session, domain);
        }

        return FULL_PUSHDOWN.apply(session, simplifiedDomain);
    };

    @Inject
    public VerticaClient(BaseJdbcConfig config, JdbcStatisticsConfig statisticsConfig, ConnectionFactory connectionFactory, QueryBuilder queryBuilder, TypeManager typeManager, IdentifierMapping identifierMapping, RemoteQueryModifier remoteQueryModifier)
    {
        super("\"", connectionFactory, queryBuilder, config.getJdbcTypesMappedToVarchar(), identifierMapping, remoteQueryModifier, true);
        ImmutableList.Builder<String> tableTypes = ImmutableList.builder();
        tableTypes.add("TABLE", "VIEW", "SYSTEM TABLE");
        this.tableTypes = tableTypes.build();
        this.statisticsEnabled = statisticsConfig.isEnabled();
        log.info("statisticsEnabled:" + this.statisticsEnabled);
        this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
        this.intervalDayToSec = typeManager.getType(new TypeSignature(StandardTypes.INTERVAL_DAY_TO_SECOND));
        this.intervalYearToMonth = typeManager.getType(new TypeSignature(StandardTypes.INTERVAL_YEAR_TO_MONTH));
        this.connectorExpressionRewriter = JdbcConnectorExpressionRewriterBuilder.newBuilder().addStandardRules(this::quoted)
                // TODO allow all comparison operators for numeric types
                .add(new RewriteComparison(ImmutableSet.of(RewriteComparison.ComparisonOperator.EQUAL, RewriteComparison.ComparisonOperator.NOT_EQUAL))).add(new RewriteIn()).withTypeClass("integer_type", ImmutableSet.of("tinyint", "smallint", "integer", "bigint")).map("$add(left: integer_type, right: integer_type)").to("left + right").map("$subtract(left: integer_type, right: integer_type)").to("left - right").map("$multiply(left: integer_type, right: integer_type)").to("left * right").map("$divide(left: integer_type, right: integer_type)").to("left / right").map("$modulus(left: integer_type, right: integer_type)").to("left % right").map("$negate(value: integer_type)").to("-value").map("$like(value: varchar, pattern: varchar): boolean").to("value LIKE pattern").map("$like(value: varchar, pattern: varchar, escape: varchar(1)): boolean").to("value LIKE pattern ESCAPE escape").map("$not($is_null(value))").to("value IS NOT NULL").map("$not(value: boolean)").to("NOT value").map("$is_null(value)").to("value IS NULL").map("$nullif(first, second)").to("NULLIF(first, second)").build();

        JdbcTypeHandle bigintTypeHandle = new JdbcTypeHandle(Types.BIGINT, Optional.of("bigint"), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
        this.aggregateFunctionRewriter = new AggregateFunctionRewriter<>(this.connectorExpressionRewriter, ImmutableSet.<AggregateFunctionRule<JdbcExpression, ParameterizedExpression>>builder().add(new ImplementCountAll(bigintTypeHandle)).add(new ImplementMinMax(false)).add(new ImplementCount(bigintTypeHandle)).add(new ImplementCountDistinct(bigintTypeHandle, false)).add(new ImplementSum(VerticaClient::toTypeHandle)).add(new ImplementAvgFloatingPoint()).add(new ImplementStddevSamp()).add(new ImplementStddevPop()).add(new ImplementVarianceSamp()).add(new ImplementVariancePop()).add(new ImplementCovarianceSamp()).add(new ImplementCovariancePop()).add(new ImplementCorr()).add(new ImplementRegrIntercept()).add(new ImplementRegrSlope()).build());
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName().orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));
        log.info("typeHandle(1):" + typeHandle.toString());
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName) {
            case "money":
                return Optional.of(moneyColumnMapping());
            case "uuid":
            case "Uuid":
                return Optional.of(uuidColumnMapping());
            case "Interval Day to Second":
                return Optional.of(intervalDayToSecColumnMapping());
            case "Interval Year to Month":
                return Optional.of(intervalYearToMonthColumnMapping());
            case "TimeTz":
                return Optional.of(timewithTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));
            case "TimestampTz":
                return Optional.of(timestampWithTimeZoneColumnMapping(typeHandle.getRequiredDecimalDigits()));
        }
        switch (typeHandle.getJdbcType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.NUMERIC:
            case Types.DECIMAL: {
                // example: typeHandle(1):JdbcTypeHandle{jdbcType=2, jdbcTypeName=Numeric, columnSize=18, decimalDigits=8}
                int columnSize = typeHandle.getRequiredColumnSize();
                int precision;
                int decimalDigits = typeHandle.getDecimalDigits().orElse(0);
                precision = columnSize + max(-decimalDigits, 0); // Map decimal(p, -s) (negative scale) to decimal(p+s, 0).
                log.info("decimal type (" + precision + "," + max(decimalDigits, 0) + ")");
                return Optional.of(decimalColumnMapping(createDecimalType(precision, max(decimalDigits, 0)), UNNECESSARY));
            }

            case Types.CHAR:
                return Optional.of(charColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.VARCHAR:
                if (!jdbcTypeName.equalsIgnoreCase("varchar")) {
                    // This can be e.g. an ENUM
                    return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
                }
                return Optional.of(varcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.LONGVARCHAR:
                return Optional.of(longVarcharColumnMapping(typeHandle.getRequiredColumnSize()));

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return Optional.of(varbinaryColumnMapping());

            case Types.DATE:
                return Optional.of(ColumnMapping.longMapping(
                        DATE,
                        sqlServerDateReadFunction(),
                        sqlServerDateWriteFunction()));

            case Types.TIME:
                return Optional.of(timeColumnMapping(typeHandle.getRequiredDecimalDigits()));

            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMappingUsingSqlTimestampWithRounding(TIMESTAMP_MICROS));

            case Types.ARRAY:
                Optional<ColumnMapping> columnMapping = arrayToTrinoType(session, connection, typeHandle);
                if (columnMapping.isPresent()) {
                    return columnMapping;
                }
                break;
        }
        log.debug("typeHandle(2):notSupported:" + typeHandle.toString());
        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    private static ColumnMapping longVarcharColumnMapping(int columnSize)
    {
        // Disable pushdown to avoid "The data types ntext and nvarchar are incompatible in the equal to operator." error
        if (columnSize > VarcharType.MAX_LENGTH) {
            return ColumnMapping.sliceMapping(createUnboundedVarcharType(), varcharReadFunction(createUnboundedVarcharType()), nvarcharWriteFunction(), DISABLE_PUSHDOWN);
        }
        return ColumnMapping.sliceMapping(createVarcharType(columnSize), varcharReadFunction(createVarcharType(columnSize)), nvarcharWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction nvarcharWriteFunction()
    {
        return (statement, index, value) -> {
            statement.setString(index, value.toStringUtf8());
        };
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }

            return builder.build();
        });
    }

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");

    private static LongWriteFunction sqlServerDateWriteFunction()
    {
        return (statement, index, day) -> statement.setString(index, DATE_FORMATTER.format(LocalDate.ofEpochDay(day)));
    }

    private static LongReadFunction sqlServerDateReadFunction()
    {
        return (resultSet, index) -> LocalDate.parse(resultSet.getString(index), DATE_FORMATTER).toEpochDay();
    }

    private static LongWriteFunction shortTimestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }

    @Override
    public List<ColumnMapping> toColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles)
    {
        return super.toColumnMappings(session, typeHandles);
    }

    private static ObjectReadFunction longTimestampWithTimeZoneReadFunction()
    {
        return ObjectReadFunction.of(
                LongTimestampWithTimeZone.class,
                (resultSet, columnIndex) -> {
                    Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                    ZonedDateTime zdtKolkata = ZonedDateTime.now();
                    ZoneOffset zoneOffSet = ZoneOffset.of(zdtKolkata.getOffset().getId());
                    System.out.println(zdtKolkata);
//                    Instant time = Instant.ofEpochMilli(timestamp.getTime());
                    OffsetDateTime odt = OffsetDateTime.of(timestamp.toLocalDateTime(), zoneOffSet);
                    return LongTimestampWithTimeZone.fromEpochSecondsAndFraction(
                            odt.toEpochSecond(),
                            (long) odt.getNano() * PICOSECONDS_PER_NANOSECOND,
                            UTC_KEY);
                });
    }

    private static ObjectWriteFunction longTimestampWithTimeZoneWriteFunction()
    {
        return ObjectWriteFunction.of(
                LongTimestampWithTimeZone.class,
                (statement, index, value) -> {
                    long epochSeconds = floorDiv(value.getEpochMillis(), MILLISECONDS_PER_SECOND);
                    long nanosOfSecond = floorMod(value.getEpochMillis(), MILLISECONDS_PER_SECOND) * NANOSECONDS_PER_MILLISECOND + value.getPicosOfMilli() / PICOSECONDS_PER_NANOSECOND;
                    Instant time = OffsetDateTime.ofInstant(Instant.ofEpochSecond(epochSeconds, nanosOfSecond), UTC_KEY.getZoneId()).toInstant();
                    ZonedDateTime zdtKolkata = ZonedDateTime.now();
                    ZoneOffset zoneOffSet = ZoneOffset.of(zdtKolkata.getOffset().getId());
                    time = time.plusSeconds(zoneOffSet.getTotalSeconds());
                    String stringTime = time.toString().replace("T", " ");
                    stringTime = stringTime.replace("Z", "");
                    Timestamp ts = Timestamp.valueOf(stringTime);
                    statement.setTimestamp(index, ts);
                });
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == BOOLEAN) {
            return WriteMapping.booleanMapping("boolean", booleanWriteFunction());
        }

        if (type == TINYINT) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }

        if (type == REAL) {
            return WriteMapping.longMapping("real", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof DecimalType decimalType) {
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.objectMapping(dataType, longDecimalWriteFunction(decimalType));
        }

        if (type instanceof CharType) {
            return WriteMapping.sliceMapping("char(" + ((CharType) type).getLength() + ")", charWriteFunction());
        }

        if (type instanceof VarcharType varcharType) {
            String dataType;
            if (varcharType.isUnbounded()) {
                dataType = "varchar";
            }
            else if (!varcharType.isUnbounded() && varcharType.getBoundedLength() > 65000) {
                dataType = "long varchar(" + varcharType.getBoundedLength() + ")";
                return WriteMapping.sliceMapping(dataType, nvarcharWriteFunction());
            }
            else {
                dataType = "varchar(" + varcharType.getBoundedLength() + ")";
            }
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("varbinary", varbinaryWriteFunction());
        }
        if (type == DATE) {
            return WriteMapping.longMapping("date", sqlServerDateWriteFunction());
        }

        if (type instanceof TimeType timeType) {
            if (timeType.getPrecision() <= VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                return WriteMapping.longMapping(format("time(%s)", timeType.getPrecision()), timeWriteFunction(timeType.getPrecision()));
            }
            return WriteMapping.longMapping(format("time(%s)", VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWriteFunction(VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION));
        }

        if (type instanceof TimeWithTimeZoneType timeWithTimeZoneType) {
            if (timeWithTimeZoneType.getPrecision() <= VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                String dataType = format("timetz(%d)", timeWithTimeZoneType.getPrecision());
                if (timeWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, timeWithTimeZoneWriteFunction(timeWithTimeZoneType.getPrecision()));
                }
            }
            return WriteMapping.longMapping(format("timetz(%d)", VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION), timeWithTimeZoneWriteFunction(timeWithTimeZoneType.getPrecision()));
        }

        if (type instanceof TimestampWithTimeZoneType timestampWithTimeZoneType) {
            if (timestampWithTimeZoneType.getPrecision() <= VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                String dataType = format("timestamptz(%d)", timestampWithTimeZoneType.getPrecision());
                if (timestampWithTimeZoneType.getPrecision() <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
                    return WriteMapping.longMapping(dataType, shortTimestampWithTimeZoneWriteFunction());
                }
                return WriteMapping.objectMapping(dataType, longTimestampWithTimeZoneWriteFunction());
            }
            return WriteMapping.objectMapping(format("timestamptz(%d)", VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWithTimeZoneWriteFunction());
        }
        if (type instanceof TimestampType timestampType) {
            if (timestampType.getPrecision() <= VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION) {
                verify(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION);
                return WriteMapping.longMapping(format("timestamp(%s)", timestampType.getPrecision()), VerticaClient::shortTimestampWriteFunction);
            }
            verify(timestampType.getPrecision() > TimestampType.MAX_SHORT_PRECISION);
            return WriteMapping.objectMapping(format("timestamp(%s)", VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION), longTimestampWriteFunction());
        }

        if (type.equals(intervalDayToSec)) {
            return WriteMapping.longMapping("Interval Day TO Second", intervalWriteFunction());
        }

        if (type.equals(intervalYearToMonth)) {
            return WriteMapping.longMapping("Interval Year TO Month", intervalYearToMonthWriteFunction());
        }

        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType arrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = arrayType.getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementTypeName(session, this, elementType)));
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setString(index, jdbcArray.toString());
        });
    }

    private static ColumnMapping charColumnMapping(int charLength)
    {
        if (charLength > CharType.MAX_LENGTH) {
            return varcharColumnMapping(charLength);
        }
        CharType charType = createCharType(charLength);
        return ColumnMapping.sliceMapping(charType, charReadFunction(charType), charWriteFunction(), VERTICA_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping varcharColumnMapping(int varcharLength)
    {
        VarcharType varcharType = varcharLength <= VarcharType.MAX_LENGTH ? createVarcharType(varcharLength) : createUnboundedVarcharType();
        return ColumnMapping.sliceMapping(varcharType, varcharReadFunction(varcharType), varcharWriteFunction(), VERTICA_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static ColumnMapping typedVarcharColumnMapping(String jdbcTypeName)
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName),
                VERTICA_STRING_COLLATION_AWARE_PUSHDOWN);
    }

    private static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName)
    {
        String bindExpression = format("CAST(? AS %s)", requireNonNull(jdbcTypeName, "jdbcTypeName is null"));

        return new SliceWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, Slice value)
                    throws SQLException
            {
                statement.setString(index, value.toStringUtf8());
            }
        };
    }

    private static ColumnMapping moneyColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                VARCHAR,
                new SliceReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // super calls ResultSet#getObject(), which for money type calls .getDouble and the call may fail to parse the money value.
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public Slice readSlice(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        return utf8Slice(resultSet.getString(columnIndex));
                    }
                },
                (statement, index, value) -> { throw new TrinoException(NOT_SUPPORTED, "Money type is not supported for INSERT"); },
                DISABLE_PUSHDOWN);
    }

    private static ColumnMapping timestampColumnMappingUsingSqlTimestampWithRounding(TimestampType timestampType)
    {
        // TODO support higher precision
        checkArgument(timestampType.getPrecision() <= TimestampType.MAX_SHORT_PRECISION, "Precision is out of range: %s", timestampType.getPrecision());
        return ColumnMapping.longMapping(timestampType, (resultSet, columnIndex) -> {
            LocalDateTime localDateTime = resultSet.getTimestamp(columnIndex).toLocalDateTime();
            int roundedNanos = toIntExact(round(localDateTime.getNano(), 9 - timestampType.getPrecision()));
            LocalDateTime rounded = localDateTime.withNano(0).plusNanos(roundedNanos);
            return toTrinoTimestamp(timestampType, rounded);
        }, timestampWriteFunction(timestampType), FULL_PUSHDOWN);
    }

    private static ColumnMapping timeColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision);
        return ColumnMapping.longMapping(createTimeType(precision), (resultSet, columnIndex) -> {
            LocalTime time = LocalTime.parse(resultSet.getObject(columnIndex, String.class));
            long nanosOfDay = time.toNanoOfDay();
            if (nanosOfDay == NANOSECONDS_PER_DAY - 1) {
                nanosOfDay = NANOSECONDS_PER_DAY - LongMath.pow(10, 9 - precision);
            }

            long picosOfDay = nanosOfDay * PICOSECONDS_PER_NANOSECOND;
            return round(picosOfDay, 12 - precision);
        }, timeWriteFunction(precision), DISABLE_PUSHDOWN);
    }

    private static ColumnMapping timewithTimeZoneColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision);
        return ColumnMapping.longMapping(createTimeWithTimeZoneType(precision), (resultSet, columnIndex) -> {
            LocalTime time = LocalTime.parse(resultSet.getObject(columnIndex, String.class));
            ZonedDateTime zdtKolkata = ZonedDateTime.now();
            ZoneOffset zoneOffSet = ZoneOffset.of(zdtKolkata.getOffset().getId());
            time = time.minusSeconds(zoneOffSet.getTotalSeconds());
            long nanosOfDay = time.toNanoOfDay();
            long picosOfDay = nanosOfDay;
            if (precision != 0) {
                picosOfDay = round(picosOfDay, 12 - precision);
            }
            long pack = packTimeNanos(picosOfDay);
            return pack;
        }, timeWithTimeZoneWriteFunction(precision), DISABLE_PUSHDOWN);
    }

    public static long packTimeNanos(long packedTimeWithTimeZone)
    {
        return packedTimeWithTimeZone << 11;
    }

    private static ColumnMapping timestampWithTimeZoneColumnMapping(int precision)
    {
        verify(precision <= 6, "Unsupported precision: %s", precision);
        TimestampWithTimeZoneType trinoType = createTimestampWithTimeZoneType(precision);
        if (precision <= TimestampWithTimeZoneType.MAX_SHORT_PRECISION) {
            return ColumnMapping.longMapping(
                    trinoType,
                    shortTimestampWithTimeZoneReadFunction(),
                    shortTimestampWithTimeZoneWriteFunction());
        }
        return ColumnMapping.objectMapping(
                trinoType,
                longTimestampWithTimeZoneReadFunction(),
                longTimestampWithTimeZoneWriteFunction());
    }

    private static LongReadFunction shortTimestampWithTimeZoneReadFunction()
    {
        return (resultSet, columnIndex) -> {
            long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
            return packMillisUtc(millisUtc);
        };
    }

    private static final int MILLIS_SHIFT = 12;

    public static long packMillisUtc(long dateTimeWithTimeZone)
    {
        return dateTimeWithTimeZone << MILLIS_SHIFT;
    }

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter TIME_FORMATTER2 = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSZ");

    private static LongWriteFunction timeWriteFunction(int precision)
    {
        checkArgument(precision <= 6, "Unsupported precision: %s", precision);
        String bindExpression = format("CAST(? AS time(%s))", precision);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = round(picosOfDay, 12 - precision);
                if (picosOfDay == PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay / PICOSECONDS_PER_NANOSECOND);
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, TIME_FORMATTER.format(localTime));
            }
        };
    }

    private static LongWriteFunction timeWithTimeZoneWriteFunction(int precision)
    {
        checkArgument(precision <= 6, "Unsupported precision: %s", precision);
        String bindExpression = format("CAST(? AS timetz(%s))", precision);
        return new LongWriteFunction()
        {
            @Override
            public String getBindExpression()
            {
                return bindExpression;
            }

            @Override
            public void set(PreparedStatement statement, int index, long picosOfDay)
                    throws SQLException
            {
                picosOfDay = unpackTimeNanos(picosOfDay);
                if (precision != 0) {
                    picosOfDay = round(picosOfDay, 12 - precision);
                }
                if (picosOfDay == PICOSECONDS_PER_DAY) {
                    picosOfDay = 0;
                }
                LocalTime localTime = LocalTime.ofNanoOfDay(picosOfDay);
                ZonedDateTime zdtKolkata = ZonedDateTime.now();
                ZoneOffset zoneOffSet = ZoneOffset.of(zdtKolkata.getOffset().getId());
                localTime = localTime.plusSeconds(zoneOffSet.getTotalSeconds());
                // statement.setObject(.., localTime) would yield incorrect end result for 23:59:59.999000
                statement.setString(index, TIME_FORMATTER.format(localTime));
            }
        };
    }

    private static void shortTimestampWriteFunction(PreparedStatement statement, int index, long epochMicros)
            throws SQLException
    {
        LocalDateTime localDateTime = fromTrinoTimestamp(epochMicros);
        statement.setTimestamp(index, Timestamp.valueOf(localDateTime));
    }

    private static ObjectWriteFunction longTimestampWriteFunction()
    {
        return ObjectWriteFunction.of(LongTimestamp.class, (statement, index, timestamp) -> {
            // VERTICA supports up to 6 digits of precision
            //noinspection ConstantConditions
            verify(VERTICAAQ_MAX_SUPPORTED_TIMESTAMP_PRECISION == 6);

            long epochMicros = timestamp.getEpochMicros();
            if (timestamp.getPicosOfMicro() >= PICOSECONDS_PER_MICROSECOND / 2) {
                epochMicros++;
            }
            shortTimestampWriteFunction(statement, index, epochMicros);
        });
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        // TODO support complex ConnectorExpressions
        return aggregateFunctionRewriter.rewrite(session, aggregate, assignments);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<AggregateFunction> aggregates, Map<String, ColumnHandle> assignments, List<List<ColumnHandle>> groupingSets)
    {
        return true;
    }

    @Override
    public Optional<ParameterizedExpression> convertPredicate(ConnectorSession session, ConnectorExpression expression, Map<String, ColumnHandle> assignments)
    {
        return connectorExpressionRewriter.rewrite(session, expression, assignments);
    }

    private static Optional<JdbcTypeHandle> toTypeHandle(DecimalType decimalType)
    {
        return Optional.of(new JdbcTypeHandle(Types.NUMERIC, Optional.of("decimal"), Optional.of(decimalType.getPrecision()), Optional.of(decimalType.getScale()), Optional.empty(), Optional.empty()));
    }

    @Override
    public boolean supportsTopN(ConnectorSession session, JdbcTableHandle handle, List<JdbcSortItem> sortOrder)
    {
        for (JdbcSortItem sortItem : sortOrder) {
            Type sortItemType = sortItem.getColumn().getColumnType();
            if (sortItemType instanceof CharType || sortItemType instanceof VarcharType) {
                if (!isCollatable(sortItem.getColumn())) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    protected Optional<TopNFunction> topNFunction()
    {
        return Optional.of((query, sortItems, limit) -> {
            String orderBy = sortItems.stream().map(sortItem -> {
                String ordering = sortItem.getSortOrder().isAscending() ? "ASC" : "DESC";
//                String nullsHandling = sortItem.getSortOrder().isNullsFirst() ? "NULLS FIRST" : "NULLS LAST";
                String collation = "";
                return format("%s %s %s ", quoted(sortItem.getColumn().getColumnName()), collation, ordering);
            }).collect(joining(", "));
            return format("%s ORDER BY %s LIMIT %d", query, orderBy, limit);
        });
    }

    @Override
    public boolean isTopNGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed(ConnectorSession session)
    {
        return true;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle)
    {
        if (!statisticsEnabled) {
            return TableStatistics.empty();
        }
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        try {
            return readTableStatistics(session, handle);
        }
        catch (SQLException | RuntimeException e) {
            throwIfInstanceOf(e, TrinoException.class);
            throw new TrinoException(JDBC_ERROR, "Failed fetching statistics for table: " + handle, e);
        }
    }

    private TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session); Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                // Table not found
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();
            if (rowCount == -1) {
                // Table has never yet been vacuumed or analyzed
                return TableStatistics.empty();
            }
            TableStatistics.Builder tableStatistics = TableStatistics.builder();
            tableStatistics.setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream().collect(toImmutableMap(ColumnStatisticsResult::getColumnName, identity()));

            for (JdbcColumnHandle column : this.getColumns(session, table)) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }
                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.getNullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.getDistinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    if (distinctValuesIndicator >= 0.0) {
                                        return distinctValuesIndicator;
                                    }
                                    return -distinctValuesIndicator * rowCount;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(Estimate.of(rowCount * result.getAverageColumnLength().get()))
                        .setRange(toRange(result.getMin(), result.getMax(), column.getColumnType()))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static Optional<DoubleRange> toRange(Optional<Object> min, Optional<Object> max, Type columnType)
    {
        if (columnType instanceof VarcharType) {
            return Optional.empty();
        }
        if (columnType instanceof CharType) {
            return Optional.empty();
        }
        if (min.isEmpty() || max.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DoubleRange(toDouble(min.get(), columnType), toDouble(max.get(), columnType)));
    }

    private static double toDouble(Object value, Type columnType)
    {
        if (value instanceof String && columnType.equals(DATE)) {
            return LocalDate.parse((CharSequence) value).toEpochDay();
        }
        if (value instanceof Number) {
            if (columnType.equals(BIGINT) || columnType.equals(INTEGER) || columnType.equals(DATE)) {
                return ((Number) value).longValue();
            }
            if (columnType.equals(DOUBLE) || columnType instanceof DecimalType) {
                return ((Number) value).doubleValue();
            }
        }
        if (value instanceof String) {
            if (columnType.equals(BIGINT) || columnType.equals(INTEGER) || columnType.equals(DATE)) {
                return Long.parseLong(String.valueOf(value));
            }
            if (columnType.equals(DOUBLE) || columnType instanceof DecimalType) {
                return Double.parseDouble(String.valueOf(value));
            }
        }

        throw new IllegalArgumentException("unsupported column type " + columnType);
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        String schemaName = remoteTableName.getSchemaName().orElse(null);
        Optional<Long> rowCount = statisticsDao.getRowCount(schemaName, remoteTableName.getTableName());
        log.info("rowCount for " + schemaName + "." + remoteTableName.getTableName() + ":" + rowCount.toString());
        if (rowCount.isEmpty()) {
            // Table not found
            return Optional.empty();
        }
        return rowCount;
    }

    @Override
    public Optional<PreparedQuery> implementJoin(ConnectorSession session, JoinType joinType, PreparedQuery leftSource, PreparedQuery rightSource, List<JdbcJoinCondition> joinConditions, Map<JdbcColumnHandle, String> rightAssignments, Map<JdbcColumnHandle, String> leftAssignments, JoinStatistics statistics)
    {
        if (joinType == JoinType.FULL_OUTER) {
            // FULL JOIN is only supported with merge-joinable or hash-joinable join conditions
            return Optional.empty();
        }
        return implementJoinCostAware(session, joinType, leftSource, rightSource, statistics, () -> super.implementJoin(session, joinType, leftSource, rightSource, joinConditions, rightAssignments, leftAssignments, statistics));
    }

    @Override
    protected boolean isSupportedJoinCondition(ConnectorSession session, JdbcJoinCondition joinCondition)
    {
        boolean isVarchar = Stream.of(joinCondition.getLeftColumn(), joinCondition.getRightColumn())
                .map(JdbcColumnHandle::getColumnType)
                .anyMatch(type -> type instanceof CharType || type instanceof VarcharType);
        if (isVarchar) {
            // Vertica is case sensitive by default, but orders varchars differently
            JoinCondition.Operator operator = joinCondition.getOperator();
            switch (operator) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    return isEnableStringPushdownWithCollate(session);
                case EQUAL:
                case NOT_EQUAL:
                case IS_DISTINCT_FROM:
                    return true;
            }
            return false;
        }

        return true;
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(uuidType, (resultSet, columnIndex) -> javaUuidToTrinoUuid((UUID) resultSet.getObject(columnIndex)), uuidWriteFunction());
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> statement.setObject(index, trinoUuidToJavaUuid(value), SqlType.TYPE_SQL_GUID);
    }

    private ColumnMapping intervalDayToSecColumnMapping()
    {
        return ColumnMapping.longMapping(intervalDayToSec, intervalDayToSecFunction(), intervalWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static LongReadFunction intervalDayToSecFunction()
    {
        return (resultSet, index) -> {
            String data = resultSet.getObject(index, String.class);
            Long day = Long.parseLong(String.valueOf(Math.abs(Integer.parseInt(data.split(" ")[0])))) * 24 * 3600000;
            String time = data.split(" ")[1];
            String sgn = "";
            if (data.split(" ")[0].contains("-")) {
                sgn = "-";
            }
            long a = 0;
            int[]multiplier = {3600000, 60000, 1000};
            String[]splits = time.split("\\.")[0].split(":");

            for (int x = 0; x < splits.length; x++) {
                a += (Integer.parseInt(splits[x]) * ((long) multiplier[x]));
            }
            return Long.parseLong(sgn + String.valueOf(a + day));
        };
    }

    private ColumnMapping intervalYearToMonthColumnMapping()
    {
        return ColumnMapping.longMapping(intervalYearToMonth, intervalYearToMonthFunction(), intervalYearToMonthWriteFunction(), DISABLE_PUSHDOWN);
    }

    private static LongReadFunction intervalYearToMonthFunction()
    {
        return (resultSet, index) -> {
            String data = resultSet.getObject(index, String.class);
            Long day = 0L;
            Long year = 0L;
            String sgn = "";
            if (data.split(" ")[0].startsWith("-")) {
                sgn = "-";
                year = Long.parseLong(data.split("-")[1]) * 12;
                day = Long.parseLong(data.split("-")[2]);
            }
            else {
                year = Long.parseLong(data.split("-")[0]) * 12;
                day = Long.parseLong(data.split("-")[1]);
            }
            return Long.parseLong(sgn + String.valueOf(year + day));
        };
    }

    public LongWriteFunction intervalYearToMonthWriteFunction()
    {
        return (statement, index, picosOfDay) -> {
            StringBuilder buf = new StringBuilder(20);
            String sgn = "";
            if (picosOfDay < 0) {
                sgn = "-";
                picosOfDay = Math.abs(picosOfDay);
            }
            buf.append(sgn);
            long year = picosOfDay / 12;
            long month = picosOfDay % 12;
            buf.append(year);
            buf.append("-");
            buf.append(month);
            statement.setString(index, buf.toString());
        };
    }

    public LongWriteFunction intervalWriteFunction()
    {
        return (statement, index, picosOfDay) -> {
            StringBuilder buf = new StringBuilder(20);
            String sgn = "";
            if (picosOfDay < 0) {
                sgn = "-";
                picosOfDay = Math.abs(picosOfDay);
            }
            append(buf, sgn, 0, (picosOfDay / 3600000));
            picosOfDay %= 3600000;
            append(buf, ":", 2, (picosOfDay / 60000));
            picosOfDay %= 60000;
            append(buf, ":", 2, (picosOfDay / 1000));
            picosOfDay %= 1000;
            append(buf, ".", 3, picosOfDay);
            statement.setString(index, buf.toString());
        };
    }

    private void append(StringBuilder tgt, String pfx, int dgt, long val)
    {
        tgt.append(pfx);
        if (dgt > 1) {
            int pad = (dgt - 1);
            for (long xa = val; xa > 9 && pad > 0; xa /= 10) {
                pad--;
            }
            for (int xa = 0; xa < pad; xa++) {
                tgt.append('0');
            }
        }
        tgt.append(val);
    }

    @Override
    public void setTableComment(ConnectorSession session, JdbcTableHandle handle, Optional<String> comment)
    {
        execute(session, buildTableCommentSql(handle.asPlainTable().getRemoteTableName(), comment));
    }

    private String buildTableCommentSql(RemoteTableName remoteTableName, Optional<String> comment)
    {
        return format("COMMENT ON TABLE %s IS %s", quoted(remoteTableName), comment.map(BaseJdbcClient::varcharLiteral).orElse("NULL"));
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Optional<Long> getRowCount(String schema, String tableName)
        {
            return handle.createQuery("select ls.\"row_count\"\n" + "        from \"v_internal\".\"vs_logical_statistics\" as ls\n" + "        left join \"v_internal\".\"vs_tables\" as t\n" + "        on ls.\"table_oid\" = t.\"oid\"\n" + "        left join \"v_internal\".\"vs_schemata\" as s\n" + "        on t.\"schema\" = s.\"oid\"\n" + "        where s.\"name\" = :schema and t.\"name\" = :table_name and ls.\"is_row_count_valid\"").bind("schema", schema).bind("table_name", tableName).mapTo(Long.class).findOne();
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            return handle.createQuery("select DISTINCT pc.name,pc.min,pc.max,pc.ndv,pc.typelen from vs_projections p join vs_projection_columns pc on p.oid = pc.proj where p.schemaname = :schema and p.anchortablename = :table_name and pc.stat_type = 'FULL'").bind("schema", schema).bind("table_name", tableName)
                    .map((rs, ctx) -> new ColumnStatisticsResult(
                            requireNonNull(rs.getString("name"), "name is null"),
                    Optional.ofNullable(rs.getObject("ndv", Float.class)),
                            Optional.ofNullable(rs.getObject("typelen", Integer.class)),
                            Optional.ofNullable(rs.getObject("min", Object.class)),
                            Optional.ofNullable(rs.getObject("max", Object.class)))).list();
        }
    }

    private static class ColumnStatisticsResult
    {
        private final String columnName;
        private final Optional<Float> nullsFraction;
        private final Optional<Float> distinctValuesIndicator;
        private final Optional<Integer> averageColumnLength;

        private final Optional<Object> min;
        private final Optional<Object> max;

        public ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength, Optional<Object> min, Optional<Object> max)
        {
            this.columnName = columnName;
            this.nullsFraction = nullsFraction;
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
            this.min = min;
            this.max = max;
        }

        public ColumnStatisticsResult(String columnName, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength, Optional<Object> min, Optional<Object> max)
        {
            this.columnName = columnName;
            this.nullsFraction = Optional.empty();
            this.distinctValuesIndicator = distinctValuesIndicator;
            this.averageColumnLength = averageColumnLength;
            this.min = min;
            this.max = max;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public Optional<Float> getNullsFraction()
        {
            return nullsFraction;
        }

        public Optional<Float> getDistinctValuesIndicator()
        {
            return distinctValuesIndicator;
        }

        public Optional<Integer> getAverageColumnLength()
        {
            return averageColumnLength;
        }

        public Optional<Object> getMin()
        {
            return min;
        }

        public Optional<Object> getMax()
        {
            return max;
        }
    }

    protected static boolean isCollatable(JdbcColumnHandle column)
    {
        if (column.getColumnType() instanceof CharType || column.getColumnType() instanceof VarcharType) {
            String jdbcTypeName = column.getJdbcTypeHandle().getJdbcTypeName()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + column.getJdbcTypeHandle()));
            return isCollatable(jdbcTypeName);
        }

        return false;
    }

    private static boolean isCollatable(String jdbcTypeName)
    {
        return "long varchar".equals(jdbcTypeName) || "char".equals(jdbcTypeName) || "varchar".equals(jdbcTypeName) || "text".equals(jdbcTypeName);
    }

    @Override
    public void truncateTable(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support truncate table");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support drop Schema");
    }

    @Override
    public OptionalLong delete(ConnectorSession session, JdbcTableHandle handle)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support delete");
    }

    @Override
    public void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new TrinoException(NOT_SUPPORTED, "This connector does not support drop Column");
    }

    private Optional<ColumnMapping> arrayToTrinoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        checkArgument(typeHandle.getJdbcType() == Types.ARRAY, "Not array type");

        VerticaSqlConfig.ArrayMapping arrayMapping = getArrayMapping(session);
        if (arrayMapping == DISABLED) {
            return Optional.empty();
        }
        JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
        String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
        if (baseElementTypeHandle.getJdbcType() == Types.BINARY) {
            return Optional.empty();
        }
        Optional<ColumnMapping> baseElementMapping = toColumnMapping(session, connection, baseElementTypeHandle);

        if (arrayMapping == AS_ARRAY) {
            if (typeHandle.getArrayDimensions().isEmpty()) {
                return Optional.empty();
            }
            return baseElementMapping
                    .map(elementMapping -> {
                        ArrayType trinoArrayType = new ArrayType(elementMapping.getType());
                        ColumnMapping arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, elementMapping, baseElementTypeName);

                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            trinoArrayType = new ArrayType(trinoArrayType);
                            arrayColumnMapping = arrayColumnMapping(session, trinoArrayType, arrayColumnMapping, baseElementTypeName);
                        }
                        return arrayColumnMapping;
                    });
        }

        throw new IllegalStateException("Unsupported array mapping type: " + arrayMapping);
    }

    private static JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        String elementType = jdbcTypeName.substring(jdbcTypeName.lastIndexOf("[") + 1, jdbcTypeName.lastIndexOf("]"));
        switch (elementType.toLowerCase(Locale.ENGLISH)) {
            case "varchar":
                return new JdbcTypeHandle(
                        Types.VARCHAR,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());

            case "bit":
                return new JdbcTypeHandle(
                        Types.BIT,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "boolean":
            case "bool":
                return new JdbcTypeHandle(
                        Types.BOOLEAN,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "int2":
            case "smallint":
                return new JdbcTypeHandle(
                        Types.SMALLINT,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "int8":
            case "integer":
            case "int4":
                return new JdbcTypeHandle(
                        Types.INTEGER,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "bigint":
                return new JdbcTypeHandle(
                        Types.BIGINT,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "double":
            case "float8":
            case "float4":
            case "float":
                return new JdbcTypeHandle(
                        Types.DOUBLE,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "numeric":
                return new JdbcTypeHandle(
                        Types.NUMERIC,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "decimal":
                return new JdbcTypeHandle(
                        Types.DECIMAL,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "char":
                return new JdbcTypeHandle(
                        Types.CHAR,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "longvarchar":
                return new JdbcTypeHandle(
                        Types.LONGVARCHAR,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "binary":
                return new JdbcTypeHandle(
                        Types.BINARY,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "varbinary":
                return new JdbcTypeHandle(
                        Types.VARBINARY,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "longvarbinary":
                return new JdbcTypeHandle(
                        Types.LONGVARBINARY,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "time":
                return new JdbcTypeHandle(
                        Types.TIME,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "timestamp":
                return new JdbcTypeHandle(
                        Types.TIMESTAMP,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "timestamptz":
                return new JdbcTypeHandle(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "timetz":
                return new JdbcTypeHandle(
                        Types.TIME_WITH_TIMEZONE,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "uuid":
                return new JdbcTypeHandle(
                        SqlType.TYPE_SQL_GUID,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "interval day to second":
                return new JdbcTypeHandle(
                        SqlType.TYPE_SQL_INTERVAL_DAY_TO_SECOND,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "interval year to month":
                return new JdbcTypeHandle(
                        SqlType.TYPE_SQL_INTERVAL_YEAR_TO_MONTH,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "money":
                return new JdbcTypeHandle(
                        790,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
            case "date":
                return new JdbcTypeHandle(
                        Types.DATE,
                        Optional.of(elementType),
                        arrayTypeHandle.getColumnSize(),
                        arrayTypeHandle.getDecimalDigits(),
                        arrayTypeHandle.getArrayDimensions(),
                        Optional.empty());
        }
        return arrayTypeHandle;
    }

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        if (tableHandle.getColumns().isPresent()) {
            return tableHandle.getColumns().get();
        }
        checkArgument(tableHandle.isNamedRelation(), "Cannot get columns for %s", tableHandle);
        SchemaTableName schemaTableName = tableHandle.getRequiredNamedRelation().getSchemaTableName();

        try (Connection connection = connectionFactory.openConnection(session)) {
            Map<String, String> arrayColumnDimensions = ImmutableMap.of();
            if (getArrayMapping(session) == AS_ARRAY) {
                arrayColumnDimensions = getArrayColumnDimensions(connection, tableHandle);
            }
            try (ResultSet resultSet = getColumns(tableHandle, connection.getMetaData())) {
                int allColumns = 0;
                List<JdbcColumnHandle> columns = new ArrayList<>();
                while (resultSet.next()) {
                    allColumns++;
                    String columnName = resultSet.getString("COLUMN_NAME");
                    JdbcTypeHandle typeHandle;
                    if (arrayColumnDimensions.size() == 0 || (arrayColumnDimensions.containsKey(columnName + "_dType") && !arrayColumnDimensions.get(columnName + "_dType").contains("array"))) {
                        typeHandle = new JdbcTypeHandle(
                                getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                                Optional.of(resultSet.getString("TYPE_NAME")),
                                getInteger(resultSet, "COLUMN_SIZE"),
                                getInteger(resultSet, "DECIMAL_DIGITS"),
                                 Optional.empty(),
                                Optional.empty());
                    }
                    else if (arrayColumnDimensions.containsKey(columnName + "_COLUMN_SIZE") && arrayColumnDimensions.get(columnName + "_COLUMN_SIZE").isEmpty()) {
                        typeHandle = new JdbcTypeHandle(
                                getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                                Optional.of(resultSet.getString("TYPE_NAME")),
                                Optional.empty(),
                                getInteger(resultSet, "DECIMAL_DIGITS"),
                                Optional.ofNullable(Integer.valueOf(arrayColumnDimensions.get(columnName))),
                                Optional.empty());
                    }
                    else {
                        typeHandle = new JdbcTypeHandle(
                                getInteger(resultSet, "DATA_TYPE").orElseThrow(() -> new IllegalStateException("DATA_TYPE is null")),
                                Optional.of(resultSet.getString("TYPE_NAME")),
                                Optional.ofNullable(Integer.valueOf(arrayColumnDimensions.get(columnName + "_COLUMN_SIZE"))),
                                getInteger(resultSet, "DECIMAL_DIGITS"),
                                Optional.ofNullable(Integer.valueOf(arrayColumnDimensions.get(columnName))),
                                Optional.empty());
                    }
                    Optional<ColumnMapping> columnMapping = toColumnMapping(session, connection, typeHandle);
                    log.debug("Mapping data type of '%s' column '%s': %s mapped to %s", schemaTableName, columnName, typeHandle, columnMapping);
                    // skip unsupported column types
                    if (columnMapping.isPresent()) {
                        boolean nullable = (resultSet.getInt("NULLABLE") != columnNoNulls);
                        Optional<String> comment = Optional.ofNullable(resultSet.getString("REMARKS"));
                        columns.add(JdbcColumnHandle.builder()
                                .setColumnName(columnName)
                                .setJdbcTypeHandle(typeHandle)
                                .setColumnType(columnMapping.get().getType())
                                .setNullable(nullable)
                                .setComment(comment)
                                .build());
                    }
                    if (columnMapping.isEmpty()) {
                        UnsupportedTypeHandling unsupportedTypeHandling = getUnsupportedTypeHandling(session);
                        verify(
                                unsupportedTypeHandling == IGNORE,
                                "Unsupported type handling is set to %s, but toColumnMapping() returned empty for %s",
                                unsupportedTypeHandling,
                                typeHandle);
                    }
                }
                if (columns.isEmpty()) {
                    // A table may have no supported columns. In rare cases a table might have no columns at all.
                    throw new TableNotFoundException(
                            schemaTableName,
                            format("Table '%s' has no supported columns (all %s columns are not supported)", schemaTableName, allColumns));
                }
                return ImmutableList.copyOf(columns);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static Map<String, String> getArrayColumnDimensions(Connection connection, JdbcTableHandle tableHandle)
            throws SQLException
    {
        String sql = "SELECT column_name as cname, character_maximum_length as csize ,ordinal_position as dims, data_type as dType FROM COLUMNS WHERE table_name=? AND table_schema=? ";

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            statement.setString(1, remoteTableName.getTableName());
            statement.setString(2, remoteTableName.getSchemaName().orElse(null));

            Map<String, String> arrayColumnDimensions = new HashMap<>();
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    arrayColumnDimensions.put(resultSet.getString("cname") + "_COLUMN_SIZE", resultSet.getString("csize"));
                    arrayColumnDimensions.put(resultSet.getString("cname"), resultSet.getString("dims"));
                    arrayColumnDimensions.put(resultSet.getString("cname") + "_dType", resultSet.getString("dType"));
                }
            }
            return arrayColumnDimensions;
        }
    }

    @Override
    public OptionalLong update(ConnectorSession session, JdbcTableHandle handle)
    {
        checkArgument(handle.isNamedRelation(), "Unable to update from synthetic table: %s", handle);
        checkArgument(handle.getLimit().isEmpty(), "Unable to update when limit is set: %s", handle);
        checkArgument(handle.getSortOrder().isEmpty(), "Unable to update when sort order is set: %s", handle);
        checkArgument(!handle.getUpdateAssignments().isEmpty(), "Unable to update when update assignments are not set: %s", handle);
        try (Connection connection = connectionFactory.openConnection(session)) {
            verify(connection.getAutoCommit());
            PreparedQuery preparedQuery = queryBuilder.prepareUpdateQuery(
                    this,
                    session,
                    connection,
                    handle.getRequiredNamedRelation(),
                    handle.getConstraint(),
                    getAdditionalPredicate(handle.getConstraintExpressions(), Optional.empty()),
                    handle.getUpdateAssignments());
            try (PreparedStatement preparedStatement = queryBuilder.prepareStatement(this, session, connection, preparedQuery, Optional.empty())) {
                int affectedRows = preparedStatement.executeUpdate();
                // In getPreparedStatement we set autocommit to false so here we need an explicit commit
//                connection.commit();
                return OptionalLong.of(affectedRows);
            }
        }
        catch (SQLException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
