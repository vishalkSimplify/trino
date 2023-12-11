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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.writer.repdef.RepLevelWriterProviders;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;
import io.trino.spi.block.ColumnarArray;
import io.trino.spi.block.ColumnarMap;
import io.trino.spi.block.RowBlock;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.parquet.ParquetTestUtils.createArrayBlock;
import static io.trino.parquet.ParquetTestUtils.createMapBlock;
import static io.trino.parquet.ParquetTestUtils.createRowBlock;
import static io.trino.parquet.ParquetTestUtils.generateGroupSizes;
import static io.trino.parquet.ParquetTestUtils.generateOffsets;
import static io.trino.parquet.writer.NullsProvider.RANDOM_NULLS;
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.RepetitionLevelWriter;
import static io.trino.parquet.writer.repdef.RepLevelWriterProvider.getRootRepetitionLevelWriter;
import static io.trino.spi.block.ArrayBlock.fromElementBlock;
import static io.trino.spi.block.ColumnarArray.toColumnarArray;
import static io.trino.spi.block.ColumnarMap.toColumnarMap;
import static io.trino.spi.block.MapBlock.fromKeyValueBlock;
import static io.trino.spi.block.RowBlock.getNullSuppressedRowFieldsFromBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRepetitionLevelWriter
{
    private static final int POSITIONS = 1024;

    @Test
    public void testWriteRowRepetitionLevels()
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            // Using an array of row blocks for testing as Structs don't have a repetition level by themselves
            Optional<boolean[]> valueIsNull = RANDOM_NULLS.getNulls(POSITIONS);
            int[] arrayOffsets = generateOffsets(valueIsNull, POSITIONS);
            int rowBlockPositions = arrayOffsets[POSITIONS];
            RowBlock rowBlock = createRowBlock(nullsProvider.getNulls(rowBlockPositions), rowBlockPositions);
            ArrayBlock arrayBlock = fromElementBlock(POSITIONS, valueIsNull, arrayOffsets, rowBlock);

            ColumnarArray columnarArray = toColumnarArray(arrayBlock);
            Block row = columnarArray.getElementsBlock();
            List<Block> nullSuppressedFields = getNullSuppressedRowFieldsFromBlock(row);
            // Write Repetition levels for all positions
            for (int fieldIndex = 0; fieldIndex < nullSuppressedFields.size(); fieldIndex++) {
                Block field = nullSuppressedFields.get(fieldIndex);
                assertRepetitionLevels(columnarArray, row, field, ImmutableList.of());
                assertRepetitionLevels(columnarArray, row, field, ImmutableList.of());

                // Write Repetition levels for all positions one-at-a-time
                assertRepetitionLevels(
                        columnarArray,
                        row,
                        field,
                        nCopies(columnarArray.getPositionCount(), 1));

                // Write Repetition levels for all positions with different group sizes
                assertRepetitionLevels(
                        columnarArray,
                        row,
                        field,
                        generateGroupSizes(columnarArray.getPositionCount()));
            }
        }
    }

    @Test
    public void testWriteArrayRepetitionLevels()
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            Block arrayBlock = createArrayBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
            ColumnarArray columnarArray = toColumnarArray(arrayBlock);
            // Write Repetition levels for all positions
            assertRepetitionLevels(columnarArray, ImmutableList.of());

            // Write Repetition levels for all positions one-at-a-time
            assertRepetitionLevels(columnarArray, nCopies(columnarArray.getPositionCount(), 1));

            // Write Repetition levels for all positions with different group sizes
            assertRepetitionLevels(columnarArray, generateGroupSizes(columnarArray.getPositionCount()));
        }
    }

    @Test
    public void testWriteMapRepetitionLevels()
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            Block mapBlock = createMapBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
            ColumnarMap columnarMap = toColumnarMap(mapBlock);
            // Write Repetition levels for all positions
            assertRepetitionLevels(columnarMap, ImmutableList.of());

            // Write Repetition levels for all positions one-at-a-time
            assertRepetitionLevels(columnarMap, nCopies(columnarMap.getPositionCount(), 1));

            // Write Repetition levels for all positions with different group sizes
            assertRepetitionLevels(columnarMap, generateGroupSizes(columnarMap.getPositionCount()));
        }
    }

    @Test
    public void testNestedStructRepetitionLevels()
    {
        for (NullsProvider nullsProvider : NullsProvider.values()) {
            RowBlock rowBlock = createNestedRowBlock(nullsProvider.getNulls(POSITIONS), POSITIONS);
            List<Block> fieldBlocks = getNullSuppressedRowFieldsFromBlock(rowBlock);

            for (int field = 0; field < fieldBlocks.size(); field++) {
                Block fieldBlock = fieldBlocks.get(field);
                ColumnarMap columnarMap = toColumnarMap(fieldBlock);
                for (Block mapElements : ImmutableList.of(columnarMap.getKeysBlock(), columnarMap.getValuesBlock())) {
                    ColumnarArray columnarArray = toColumnarArray(mapElements);

                    // Write Repetition levels for all positions
                    assertRepetitionLevels(rowBlock, columnarMap, columnarArray, ImmutableList.of());

                    // Write Repetition levels for all positions one-at-a-time
                    assertRepetitionLevels(rowBlock, columnarMap, columnarArray, nCopies(rowBlock.getPositionCount(), 1));

                    // Write Repetition levels for all positions with different group sizes
                    assertRepetitionLevels(rowBlock, columnarMap, columnarArray, generateGroupSizes(rowBlock.getPositionCount()));
                }
            }
        }
    }

    private static RowBlock createNestedRowBlock(Optional<boolean[]> rowIsNull, int positionCount)
    {
        Block[] fieldBlocks = new Block[2];
        // no nulls map block
        fieldBlocks[0] = createMapOfArraysBlock(rowIsNull, positionCount);
        // random nulls map block
        fieldBlocks[1] = createMapOfArraysBlock(RANDOM_NULLS.getNulls(positionCount, rowIsNull), positionCount);

        return RowBlock.fromNotNullSuppressedFieldBlocks(positionCount, rowIsNull, fieldBlocks);
    }

    private static Block createMapOfArraysBlock(Optional<boolean[]> mapIsNull, int positionCount)
    {
        int[] offsets = generateOffsets(mapIsNull, positionCount);
        int entriesCount = offsets[positionCount];
        Block keyBlock = createArrayBlock(Optional.empty(), entriesCount);
        Block valueBlock = createArrayBlock(RANDOM_NULLS.getNulls(entriesCount), entriesCount);
        return fromKeyValueBlock(mapIsNull, offsets, keyBlock, valueBlock, new MapType(BIGINT, BIGINT, new TypeOperators()));
    }

    private static void assertRepetitionLevels(
            ColumnarArray columnarArray,
            Block row,
            Block field,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter fieldRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, maxRepetitionLevel),
                        RepLevelWriterProviders.of(row),
                        RepLevelWriterProviders.of(field)),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            fieldRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                fieldRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        Iterator<Integer> expectedRepetitionLevelsIter = RepLevelIterables.getIterator(ImmutableList.<RepLevelIterable>builder()
                .add(RepLevelIterables.of(columnarArray, maxRepetitionLevel))
                .add(RepLevelIterables.of(columnarArray.getElementsBlock()))
                .add(RepLevelIterables.of(field))
                .build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(ImmutableList.copyOf(expectedRepetitionLevelsIter));
    }

    private static void assertRepetitionLevels(
            ColumnarArray columnarArray,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter elementsRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarArray, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            elementsRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                elementsRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        ImmutableList.Builder<Integer> expectedRepLevelsBuilder = ImmutableList.builder();
        int elementsOffset = 0;
        for (int position = 0; position < columnarArray.getPositionCount(); position++) {
            if (columnarArray.isNull(position)) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            int arrayLength = columnarArray.getLength(position);
            if (arrayLength == 0) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
            for (int i = elementsOffset + 1; i < elementsOffset + arrayLength; i++) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel);
            }
            elementsOffset += arrayLength;
        }
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
    }

    private static void assertRepetitionLevels(
            ColumnarMap columnarMap,
            List<Integer> writePositionCounts)
    {
        int maxRepetitionLevel = 1;
        // Write Repetition levels for map keys
        TestingValuesWriter keysWriter = new TestingValuesWriter();
        RepetitionLevelWriter keysRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarMap, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarMap.getKeysBlock())),
                keysWriter);
        if (writePositionCounts.isEmpty()) {
            keysRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                keysRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Write Repetition levels for map values
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter valuesRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(columnarMap, maxRepetitionLevel),
                        RepLevelWriterProviders.of(columnarMap.getValuesBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            valuesRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                valuesRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        ImmutableList.Builder<Integer> expectedRepLevelsBuilder = ImmutableList.builder();
        for (int position = 0; position < columnarMap.getPositionCount(); position++) {
            if (columnarMap.isNull(position)) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            int mapLength = columnarMap.getEntryCount(position);
            if (mapLength == 0) {
                expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
                continue;
            }
            expectedRepLevelsBuilder.add(maxRepetitionLevel - 1);
            expectedRepLevelsBuilder.addAll(nCopies(mapLength - 1, maxRepetitionLevel));
        }
        assertThat(keysWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(expectedRepLevelsBuilder.build());
    }

    private static void assertRepetitionLevels(
            RowBlock rowBlock,
            ColumnarMap columnarMap,
            ColumnarArray columnarArray,
            List<Integer> writePositionCounts)
    {
        // Write Repetition levels
        TestingValuesWriter valuesWriter = new TestingValuesWriter();
        RepetitionLevelWriter fieldRootRepLevelWriter = getRootRepetitionLevelWriter(
                ImmutableList.of(
                        RepLevelWriterProviders.of(rowBlock),
                        RepLevelWriterProviders.of(columnarMap, 1),
                        RepLevelWriterProviders.of(columnarArray, 2),
                        RepLevelWriterProviders.of(columnarArray.getElementsBlock())),
                valuesWriter);
        if (writePositionCounts.isEmpty()) {
            fieldRootRepLevelWriter.writeRepetitionLevels(0);
        }
        else {
            for (int positionsCount : writePositionCounts) {
                fieldRootRepLevelWriter.writeRepetitionLevels(0, positionsCount);
            }
        }

        // Verify written Repetition levels
        Iterator<Integer> expectedRepetitionLevelsIter = RepLevelIterables.getIterator(ImmutableList.<RepLevelIterable>builder()
                .add(RepLevelIterables.of(rowBlock))
                .add(RepLevelIterables.of(columnarMap, 1))
                .add(RepLevelIterables.of(columnarArray, 2))
                .add(RepLevelIterables.of(columnarArray.getElementsBlock()))
                .build());
        assertThat(valuesWriter.getWrittenValues()).isEqualTo(ImmutableList.copyOf(expectedRepetitionLevelsIter));
    }
}
