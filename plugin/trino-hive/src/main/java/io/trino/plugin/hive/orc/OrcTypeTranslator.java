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
package io.trino.plugin.hive.orc;

import io.trino.orc.metadata.OrcType.OrcTypeKind;
import io.trino.plugin.hive.coercions.BooleanCoercer.BooleanToVarcharCoercer;
import io.trino.plugin.hive.coercions.DateCoercer.VarcharToDateCoercer;
import io.trino.plugin.hive.coercions.DoubleToVarcharCoercer;
import io.trino.plugin.hive.coercions.IntegerNumberToDoubleCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToDateCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToLongTimestampCoercer;
import io.trino.plugin.hive.coercions.TimestampCoercer.VarcharToShortTimestampCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.plugin.hive.coercions.VarcharToDoubleCoercer;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.Optional;

import static io.trino.orc.metadata.OrcType.OrcTypeKind.BOOLEAN;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BYTE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.DOUBLE;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.SHORT;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.STRING;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.TIMESTAMP;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.VARCHAR;
import static io.trino.plugin.hive.coercions.DecimalCoercers.createIntegerNumberToDecimalCoercer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

public final class OrcTypeTranslator
{
    private OrcTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(OrcTypeKind fromOrcType, Type toTrinoType)
    {
        if (fromOrcType == TIMESTAMP) {
            if (toTrinoType instanceof VarcharType varcharType) {
                return Optional.of(new LongTimestampToVarcharCoercer(TIMESTAMP_NANOS, varcharType));
            }
            if (toTrinoType instanceof DateType toDateType) {
                return Optional.of(new LongTimestampToDateCoercer(TIMESTAMP_NANOS, toDateType));
            }
            return Optional.empty();
        }
        if (isVarcharType(fromOrcType)) {
            if (toTrinoType instanceof TimestampType timestampType) {
                if (timestampType.isShort()) {
                    return Optional.of(new VarcharToShortTimestampCoercer(createUnboundedVarcharType(), timestampType));
                }
                return Optional.of(new VarcharToLongTimestampCoercer(createUnboundedVarcharType(), timestampType));
            }
            if (toTrinoType instanceof DateType toDateType) {
                return Optional.of(new VarcharToDateCoercer(createUnboundedVarcharType(), toDateType));
            }
            if (toTrinoType instanceof DoubleType) {
                return Optional.of(new VarcharToDoubleCoercer(createUnboundedVarcharType(), true));
            }
            return Optional.empty();
        }
        if (fromOrcType == DOUBLE && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(new DoubleToVarcharCoercer(varcharType, true));
        }
        if (fromOrcType == BOOLEAN && toTrinoType instanceof VarcharType varcharType) {
            return Optional.of(new BooleanToVarcharCoercer(varcharType));
        }
        if (toTrinoType instanceof DoubleType) {
            if (fromOrcType == BYTE) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(TINYINT));
            }
            if (fromOrcType == SHORT) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(SMALLINT));
            }
            if (fromOrcType == INT) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(INTEGER));
            }
            if (fromOrcType == LONG) {
                return Optional.of(new IntegerNumberToDoubleCoercer<>(BIGINT));
            }
        }
        if (toTrinoType instanceof DecimalType decimalType) {
            if (fromOrcType == BYTE) {
                return Optional.of(createIntegerNumberToDecimalCoercer(TINYINT, decimalType));
            }
            if (fromOrcType == SHORT) {
                return Optional.of(createIntegerNumberToDecimalCoercer(SMALLINT, decimalType));
            }
            if (fromOrcType == INT) {
                return Optional.of(createIntegerNumberToDecimalCoercer(INTEGER, decimalType));
            }
            if (fromOrcType == LONG) {
                return Optional.of(createIntegerNumberToDecimalCoercer(BIGINT, decimalType));
            }
        }
        return Optional.empty();
    }

    private static boolean isVarcharType(OrcTypeKind orcTypeKind)
    {
        return orcTypeKind == STRING || orcTypeKind == VARCHAR;
    }
}
