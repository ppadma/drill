/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.record;

import static org.apache.drill.exec.record.TestVectorContainer.fixture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.RecordBatchSizer.ColumnSize;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.VectorInitializer;
import org.apache.drill.exec.record.VectorInitializer.AllocationHint;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.RepeatedIntVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSet.SingleRowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.SchemaBuilder;
import org.junit.Test;

import java.util.Map;

public class TestRecordBatchSizer extends SubOperatorTest {

  private final int testRowCount = 1000;

  private final int testRowCountPowerTwo = 2048;


  private void verifyColumnValues(ColumnSize column, int stdDataSize, int stdNetSize, int dataSize, int netSize, int totalDataSize, int totalNetSize, int valueCount, int elementCount, int estElementCountPerArray, boolean isVariableWidth) {
    assertNotNull(column);

    assertEquals(stdDataSize, column.getStdDataSize());
    assertEquals(stdNetSize, column.getStdNetSize());

    assertEquals(dataSize, column.getDataSize());
    assertEquals(netSize, column.getNetSize());

    assertEquals(totalDataSize, column.getTotalDataSize());
    assertEquals(totalNetSize, column.getTotalNetSize());

    assertEquals(valueCount, column.getValueCount());
    assertEquals(elementCount, column.getElementCount());

    assertEquals(estElementCountPerArray, column.getEstElementCountPerArray(), 0.01);
    assertEquals(isVariableWidth, column.isVariableWidth());

  }

  @Test
  public void testSizerFixedWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().add("a", MinorType.BIGINT).add("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(i, (float) i * 0.1);
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");

    verifyColumnValues(aColumn, 8, 8, 8, 8,
      80, 80, 10, 10, 1, false);

    ColumnSize bColumn = sizer.columns().get("b");
    verifyColumnValues(bColumn, 8, 8, 8, 8, 80, 80,
      10, 10, 1,false);

    VectorInitializer initializer = new VectorInitializer();
    aColumn.buildVectorInitializer(initializer);
    bColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();
      initializer.allocateVector(v, "", testRowCount);
      assertEquals((Integer.highestOneBit(testRowCount) << 1), v.getValueCapacity());
      v.clear();
      initializer.allocateVector(v, "", testRowCountPowerTwo);
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo), v.getValueCapacity());
      v.clear();
      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT);
      assertEquals(ValueVector.MAX_ROW_COUNT, v.getValueCapacity());
      v.clear();
      initializer.allocateVector(v, "", 0);
      assertEquals(ValueVector.MIN_ROW_COUNT, v.getValueCapacity());
      v.clear();
    }

    rows.clear();
    empty.clear();
  }


  @Test
  public void testSizerRepeatedFixedWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().addArray("a", MinorType.BIGINT).addArray("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(new long[] {1, 2, 3, 4, 5}, new double[] {(double)i*0.1, (double)i*0.1, (double)i*0.1, (double)i*0.2, (double)i*0.3});
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");

    ColumnSize bColumn = sizer.columns().get("b");

    verifyColumnValues(sizer.columns().get("a"),
      40, 44, 40, 44, 400, 440, 10, 50, 5, false);

    verifyColumnValues(sizer.columns().get("b"),
      40, 44, 40, 44, 400, 440, 10, 50, 5, false);

    VectorInitializer initializer = new VectorInitializer();
    aColumn.buildVectorInitializer(initializer);
    bColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;
    ValueVector dataVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit((testRowCount * 5)  << 1), dataVector.getValueCapacity());
      v.clear();


      initializer.allocateVector(v, "", testRowCountPowerTwo - 1);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo)), offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit((testRowCountPowerTwo -1) * 5) << 1, dataVector.getValueCapacity());
      v.clear();


      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT - 1);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(Integer.highestOneBit((ValueVector.MAX_ROW_COUNT * 5) << 1), dataVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", 0);
      offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      dataVector = ((RepeatedValueVector) v).getDataVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, dataVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }

  @Test
  public void testSizerNullableFixedWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().addNullable("a", MinorType.BIGINT).addNullable("b", MinorType.FLOAT8).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (long i = 0; i < 10; i++) {
      builder.addRow(i, (float)i*0.1);
    }

    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(2, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");
    ColumnSize bColumn = sizer.columns().get("b");

    verifyColumnValues(aColumn,
      8, 9, 8, 9, 80, 90, 10, 10, 1, false);

    verifyColumnValues(bColumn,
      8, 9, 8, 9, 80, 90, 10, 10, 1, false);

    VectorInitializer initializer = new VectorInitializer();
    aColumn.buildVectorInitializer(initializer);
    bColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    ValueVector bitVector, valueVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(Integer.highestOneBit(testRowCount << 1), valueVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", testRowCountPowerTwo);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo)), bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo), valueVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, valueVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", 0);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, bitVector.getValueCapacity());
      valueVector = ((NullableVector) v).getValuesVector();
      assertEquals(ValueVector.MIN_ROW_COUNT, valueVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }

  @Test
  public void testSizerVariableWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().add("a", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    StringBuilder stringBuilder = new StringBuilder();

    for (long i = 0; i < 10; i++) {
      stringBuilder.append("a");
      builder.addRow(stringBuilder.toString());
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    ColumnSize aColumn = sizer.columns().get("a");

    verifyColumnValues(aColumn,
      50, 54, 6, 10, 55, 95, 10, 10, 1, true);

    VectorInitializer initializer = new VectorInitializer();
    aColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, v.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", testRowCountPowerTwo - 1);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo) - 1, v.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT - 1);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(ValueVector.MAX_ROW_COUNT, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MAX_ROW_COUNT - 1, v.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", 0);
      offsetVector = ((VariableWidthVector)v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, v.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerRepeatedVariableWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().addArray("b", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    String[] newString = new String [] {"a", "aa", "aaa", "aaaa", "aaaaa"};

    for (long i = 0; i < 10; i++) {
      builder.addRow((Object) (newString));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    ColumnSize bColumn = sizer.columns().get("b");

    verifyColumnValues(bColumn,
      250, 258, 15, 39, 150,
      390, 10, 50, 5,true);

    VectorInitializer initializer = new VectorInitializer();
    bColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;
    VariableWidthVector vwVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());

      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = ((VariableWidthVector)vwVector).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", testRowCountPowerTwo);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo) << 1), offsetVector.getValueCapacity());

      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = ((VariableWidthVector)vwVector).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT) << 1), offsetVector.getValueCapacity());

      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = ((VariableWidthVector)vwVector).getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT * 5) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(ValueVector.MAX_ROW_COUNT * 5  << 1)-1, vwVector.getValueCapacity());
      v.clear();

      initializer.allocateVector(v, "", 0);
      offsetVector = ((RepeatedValueVector)v).getOffsetVector();
      assertEquals(ValueVector.MIN_ROW_COUNT + 1, offsetVector.getValueCapacity());

      vwVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
      offsetVector = ((VariableWidthVector)vwVector).getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MIN_ROW_COUNT ) << 1), offsetVector.getValueCapacity());
      assertEquals(ValueVector.MIN_ROW_COUNT, vwVector.getValueCapacity());
      v.clear();
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerNullableVariableWidth() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder().addNullable("b", MinorType.VARCHAR).build();
    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    StringBuilder stringBuilder = new StringBuilder();

    for (long i = 0; i < 10; i++) {
      stringBuilder.append("a");
      builder.addRow( (Object) stringBuilder.toString());
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    verifyColumnValues(sizer.columns().get("b"),
      50, 55, 6, 11, 55, 105, 10, 10, 1,true);

    VectorInitializer initializer = new VectorInitializer();

    ColumnSize bColumn = sizer.columns().get("b");
    bColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    ValueVector bitVector, valueVector;
    VariableWidthVector vwVector;
    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, vwVector.getValueCapacity());

      initializer.allocateVector(v, "", testRowCountPowerTwo);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo)), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCountPowerTwo) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCountPowerTwo  << 1)-1, vwVector.getValueCapacity());

      initializer.allocateVector(v, "", ValueVector.MAX_ROW_COUNT);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT)), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MAX_ROW_COUNT) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(ValueVector.MAX_ROW_COUNT  << 1)-1, vwVector.getValueCapacity());

      initializer.allocateVector(v, "", 0);
      bitVector = ((NullableVector) v).getBitsVector();
      assertEquals((Integer.highestOneBit(ValueVector.MIN_ROW_COUNT)), bitVector.getValueCapacity());
      vwVector = (VariableWidthVector) ((NullableVector) v).getValuesVector();
      offsetVector = vwVector.getOffsetVector();
      assertEquals((Integer.highestOneBit(ValueVector.MIN_ROW_COUNT) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(ValueVector.MIN_ROW_COUNT  << 1)-1, vwVector.getValueCapacity());
    }

    empty.clear();
    rows.clear();
  }


  @Test
  public void testSizerMap() {
    // Create a row set with less than one item, on
    // average, per array.

    BatchSchema schema = new SchemaBuilder()
      .addMap("map")
        .add("key", MinorType.INT)
        .add("value", MinorType.VARCHAR)
      .buildMap()
      .build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) (new Object[] {10, "a"}));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    verifyColumnValues(sizer.columns().get("map"),
      54,/* Fix it */  58, /* should be 44. Fix it */
      5, 9 /* should be 50. Fix it */, 50 /* should be 440. fix it */,
      90, 10 /* should be 440. fix it. */, 10, 1, true);

    VectorInitializer initializer = new VectorInitializer();

    ColumnSize mapColumn = sizer.columns().get("map");
    mapColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);

      MapVector mapVector = (MapVector)v;
      ValueVector keyVector = mapVector.getChild("key");
      ValueVector valueVector1 = mapVector.getChild("value");

      assertEquals((Integer.highestOneBit(testRowCount) << 1), keyVector.getValueCapacity());

      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, valueVector1.getValueCapacity());

      v.clear();
    }

    empty.clear();
    rows.clear();

  }

  @Test
  public void testSizerRepeatedMap() {
    BatchSchema schema = new SchemaBuilder().addMapArray("map").
      add("key", MinorType.INT).
      add("value", MinorType.VARCHAR).
      buildMap().build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) new Object[] {
        new Object[] {110, "a"},
        new Object[] {120, "b"}});
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    verifyColumnValues(sizer.columns().get("map"),
      54,/* Fix it */  62, /* should be 44. Fix it */
      5, 22 /* should be 50. Fix it */, 100 /* should be 440. fix it */,
      220, 10 /* should be 440. fix it. */, 20, 2, true);

    rows.clear();
  }

  @Test
  public void testSizerNestedMap() {

    // Create a row set with less than one item, on
    // average, per array.
    BatchSchema schema = new SchemaBuilder()
      .addMap("map")
        .add("key", MinorType.INT)
        .add("value", MinorType.VARCHAR)
        .addMap("childMap")
          .add("childKey", MinorType.INT)
          .add("childValue", MinorType.VARCHAR)
          .buildMap()
       .buildMap()
      .build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) (new Object[] {10, "a", new Object[] {5, "b"}}));
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    verifyColumnValues(sizer.columns().get("map"),
      108,/* Fix it */  116, /* should be 44. Fix it */
      10, 18 /* should be 50. Fix it */, 100 /* should be 440. fix it */,
      180, 10 /* should be 440. fix it. */, 10, 1, true);

    VectorInitializer initializer = new VectorInitializer();

    ColumnSize mapColumn = sizer.columns().get("map");
    mapColumn.buildVectorInitializer(initializer);

    SingleRowSet empty = fixture.rowSet(schema);
    VectorAccessible accessible = empty.vectorAccessible();

    UInt4Vector offsetVector;

    for (VectorWrapper<?> vw : accessible) {
      ValueVector v = vw.getValueVector();

      initializer.allocateVector(v, "", testRowCount);

      MapVector mapVector = (MapVector)v;
      ValueVector keyVector = mapVector.getChild("key");
      ValueVector valueVector1 = mapVector.getChild("value");

      assertEquals((Integer.highestOneBit(testRowCount) << 1), keyVector.getValueCapacity());

      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, valueVector1.getValueCapacity());

      MapVector childMapVector = (MapVector) mapVector.getChild("childMap");
      ValueVector childKeyVector = childMapVector.getChild("childKey");
      ValueVector childValueVector1 = childMapVector.getChild("childValue");

      assertEquals((Integer.highestOneBit(testRowCount) << 1), childKeyVector.getValueCapacity());

      offsetVector = ((VariableWidthVector)valueVector1).getOffsetVector();
      assertEquals((Integer.highestOneBit(testRowCount) << 1), offsetVector.getValueCapacity());
      assertEquals(Integer.highestOneBit(testRowCount  << 1)-1, childValueVector1.getValueCapacity());

      v.clear();
    }

    empty.clear();
    rows.clear();

  }

  @Test
  public void testSizerRepeatedList() {
    BatchSchema schema = new SchemaBuilder().addMapArray("map").
      add("key", MinorType.INT).
      add("value", MinorType.VARCHAR).
      buildMap().build();

    RowSetBuilder builder = fixture.rowSetBuilder(schema);

    for (int i = 0; i < 10; i++) {
      builder.addRow((Object) new Object[] {
        new Object[] {110, "a"},
        new Object[] {120, "b"}});
    }
    RowSet rows = builder.build();

    // Run the record batch sizer on the resulting batch.
    RecordBatchSizer sizer = new RecordBatchSizer(rows.container());
    assertEquals(1, sizer.columns().size());

    verifyColumnValues(sizer.columns().get("map"),
      54,/* Fix it */  62, /* should be 44. Fix it */
      5, 22 /* should be 50. Fix it */, 100 /* should be 440. fix it */,
      220, 10 /* should be 440. fix it. */, 20, 2, true);

    rows.clear();
  }

}