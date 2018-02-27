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

import java.util.Set;
import java.util.Map;

import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.memory.AllocationManager.BufferLedger;
import org.apache.drill.exec.memory.BaseAllocator;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;
import org.apache.drill.exec.vector.complex.RepeatedValueVector;
import org.apache.drill.exec.vector.VariableWidthVector;

import com.google.common.collect.Sets;
import org.apache.drill.exec.vector.complex.RepeatedVariableWidthVectorLike;

/**
 * Given a record batch or vector container, determines the actual memory
 * consumed by each column, the average row, and the entire record batch.
 */

public class RecordBatchSizer {

  /**
   * Column size information.
   */
  public static class ColumnSize {
    public final String prefix;
    public final MaterializedField metadata;

    /**
     * Assumed size per entry from Drill metadata, based on type.
     * This is what typeHelper would return. Note that this information is
     * just an approximation for variable-width columns.
     * It is not accurate and hence, not recommended to use for variable
     * width columns.
     */
    private int stdDataSize;

    private int stdNetSize;

    /**
     * This is the average per entry raw data size in bytes. Does not
     * include any overhead.
     */
    private int dataSize;

    /**
     * This is the average per entry size of just pure data plus
     * overhead of additional vectors we add on top like bits vector,
     * offset vector etc.
     * Actual average column width as determined from actual memory use. This
     * size is larger than the actual data size since this size includes per-
     * column overhead such as any unused vector space, etc.
     */
    private int netSize;

    private int totalDataSize; // for all entries

    private int totalNetSize; // for all entries.

    public int getStdDataSize() {
      int mapStdDataSize = this.stdDataSize;
      for (ColumnSize columnSize : children.values()) {
        mapStdDataSize += columnSize.getStdDataSize();
      }
      return mapStdDataSize;
    }

    public int getStdNetSize() {
      int mapStdNetSize = this.stdNetSize;
      for (ColumnSize columnSize : children.values()) {
        mapStdNetSize += columnSize.getStdNetSize();
      }
      return mapStdNetSize;
    }

    public int getDataSize() {
      int mapDataSize = this.dataSize;
      for (ColumnSize columnSize : children.values()) {
        mapDataSize += columnSize.getDataSize();
      }
      return mapDataSize;
    }

    public int getNetSize() {
      return RecordBatchSizer.safeDivide(getTotalNetSize(), getValueCount());
    }

    public int getTotalDataSize() {
      int mapTotalDataSize = this.totalDataSize;
      for (ColumnSize columnSize : children.values()) {
        mapTotalDataSize += columnSize.getTotalDataSize();
      }
      return mapTotalDataSize;
    }

    public int getTotalNetSize() {

      int mapTotalNetSize = this.totalNetSize;
      for (ColumnSize columnSize : children.values()) {
        mapTotalNetSize += columnSize.getTotalNetSize();
      }
      return mapTotalNetSize;

     // return totalNetSize;
    }

    public int getValueCount() {
      return valueCount;
    }

    /**
     * Number of occurrences of the value in the batch. This is trivial
     * for top-level scalars: it is the record count. For a top-level
     * repeated vector, this is the number of arrays, also the record
     * count. For a value nested inside a repeated map, it is the
     * total number of values across all maps, and may be less than,
     * greater than (but unlikely) same as the row count.
     */

    private final int valueCount;

    public int getElementCount() {
      return elementCount;
    }

    /**
     * Total number of elements for a repeated type, or same as
     * valueCount if this is a non-repeated type. That is, a batch
     * of 100 rows may have an array with 10 elements per row.
     * In this case, the element count is 1000.
     */

    private int elementCount;

    public float getEstElementCountPerArray() {
      return estElementCountPerArray;
    }

    /**
     * The estimated, average number of elements per parent value.
     * Always 1 for a non-repeated type. For a repeated type,
     * this is the average entries per array (per repeated element).
     */

    private float estElementCountPerArray;

    public boolean isVariableWidth() {
      boolean mapVariableWidth = this.isVariableWidth;
      for (ColumnSize columnSize : children.values()) {
        mapVariableWidth |= columnSize.isVariableWidth();
      }
      return mapVariableWidth;
    }

    private  boolean isVariableWidth;

    private int totalRMNetSize, netRMSize;

    public Map<String, ColumnSize> children = CaseInsensitiveMap.newHashMap();

    public Map<String, ColumnSize> getChildren() {
      return children;
    }

    public ColumnSize(ValueVector v, String prefix) {
      this.prefix = prefix;
      valueCount = v.getAccessor().getValueCount();
      metadata = v.getField();
      isVariableWidth = (v instanceof VariableWidthVector || v instanceof RepeatedVariableWidthVectorLike);

      elementCount = valueCount;
      estElementCountPerArray = 1;

      try {
        // For map, go through stdSizes of individual vectors and update.
        stdDataSize = TypeHelper.getSize(metadata.getType());
        if (isVariableWidth) {
          stdNetSize = stdDataSize;
          stdDataSize -= 4;
        } else {
          stdNetSize = stdDataSize;
        }
      } catch (Exception e) {
        // For unsupported types, just set stdSize to 0.
        stdDataSize = 0;
      }

      switch(v.getField().getDataMode()) {
        case REPEATED:
          elementCount  = getElementCount(v);
          estElementCountPerArray = valueCount == 0 ? 0 : elementCount * 1.0f / valueCount;
          stdDataSize *= estElementCountPerArray;
          stdNetSize = stdDataSize + 4;

          if (metadata.getType().getMinorType() == MinorType.MAP ||
              metadata.getType().getMinorType() == MinorType.LIST) {

            totalRMNetSize = v.getPayloadByteCount(valueCount);
            netRMSize = safeDivide(totalRMNetSize, valueCount);

            UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();

            totalNetSize = offsetVector.getPayloadByteCount(valueCount);
            netSize = safeDivide(totalNetSize, valueCount);
            return;
          }

          if (isVariableWidth) {
            UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
            int innerValueCount = offsetVector.getAccessor().get(valueCount);
            VariableWidthVector variableWidthVector = ((VariableWidthVector) ((RepeatedValueVector) v).getDataVector());
            totalDataSize = variableWidthVector.getOffsetVector().getAccessor().get(innerValueCount);
            stdNetSize += 4;
          } else {
            ValueVector dataVector = ((RepeatedValueVector) v).getDataVector();
            totalDataSize = dataVector.getPayloadByteCount(elementCount);
          }

          break;
        case OPTIONAL:
          stdNetSize += 1;

          // TestMergeJoinWithSchemaChange test cases.
          if (metadata.getType().getMinorType() == MinorType.UNION ||
            metadata.getType().getMinorType() == MinorType.LIST) {
            totalDataSize = v.getPayloadByteCount(valueCount);
          } else {
            if (isVariableWidth) {
              VariableWidthVector variableWidthVector = ((VariableWidthVector) ((NullableVector) v).getValuesVector());
              totalDataSize = variableWidthVector.getOffsetVector().getAccessor().get(valueCount);
            } else {
              if (v instanceof  NullableVector) {
                totalDataSize = ((NullableVector) v).getValuesVector().getPayloadByteCount(valueCount);
              }
            }
          }
          break;

        case REQUIRED:
          if (metadata.getType().getMinorType() == MinorType.MAP) {
            return;
          }
          if (isVariableWidth) {
            UInt4Vector  offsetVector = ((VariableWidthVector)v).getOffsetVector();
            totalDataSize = offsetVector.getAccessor().get(valueCount);
          } else {
            totalDataSize = v.getPayloadByteCount(valueCount);
          }
          break;
      }

      // The amount of memory consumed by the payload: the actual
      // data stored in the vectors.
      totalNetSize = v.getPayloadByteCount(valueCount);
      netSize = (int) (safeDivide(totalNetSize, valueCount) * estElementCountPerArray);

      dataSize = safeDivide(totalDataSize, valueCount);
    }

    @SuppressWarnings("resource")
    private int getElementCount(ValueVector v) {

      // Repeated vectors are special: they have an associated offset vector
      // that changes the value count of the contained vectors.
      UInt4Vector offsetVector = ((RepeatedValueVector) v).getOffsetVector();
      int childCount = valueCount == 0 ? 0 : offsetVector.getAccessor().get(valueCount);

      return childCount;
    }

    @SuppressWarnings("resource")
    private void buildList(ValueVector v) {
      // complex ListVector cannot be casted to RepeatedListVector.
      // check the mode.

      if (v.getField().getDataMode() != DataMode.REPEATED) {
        dataSize = v.getPayloadByteCount(valueCount);
        return;
      }
      UInt4Vector offsetVector = ((RepeatedListVector) v).getOffsetVector();
      dataSize = offsetVector.getPayloadByteCount(valueCount);

    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder()
          .append(prefix)
          .append(metadata.getName())
          .append("(type: ")
          .append(metadata.getType().getMode().name())
          .append(" ")
          .append(metadata.getType().getMinorType().name())
          .append(", count: ")
          .append(valueCount);
      if (metadata.getDataMode() == DataMode.REPEATED) {
        buf.append(", elements: ")
           .append(elementCount)
           .append(", per-array: ")
           .append(estElementCountPerArray);
      }
      buf .append(", std size: ")
          .append(stdDataSize)
          .append(", actual size: ")
          .append(getNetSize())
          .append(", data size: ")
          .append(getDataSize())
          .append(")");
      return buf.toString();
    }

    /**
     * Add a single vector initializer to a collection for the entire batch.
     * Uses the observed column size information to predict the size needed
     * when allocating a new vector for the same data. Adds a hint only for
     * variable-width or repeated types; no extra information is needed for
     * fixed width, non-repeated columns.
     *
     * @param initializer the vector initializer to hold the hints
     * for this column
     */

    public void buildVectorInitializer(VectorInitializer initializer) {
      int width = 0;
      switch(metadata.getType().getMinorType()) {
      case VAR16CHAR:
      case VARBINARY:
      case VARCHAR:

        // Subtract out the offset vector width
        width = getNetSize()- 4;

        // Subtract out the bits (is-set) vector width
        if (metadata.getDataMode() == DataMode.OPTIONAL) {
          width -= 1;
        }
        break;
      default:
        break;
      }
      String name = prefix + metadata.getName();
      if (metadata.getDataMode() == DataMode.REPEATED) {
        if (width > 0) {
          // Estimated width is width of entire column. Divide
          // by element count to get per-element size.
          initializer.variableWidthArray(name, width / estElementCountPerArray, estElementCountPerArray);
        } else {
          initializer.fixedWidthArray(name, estElementCountPerArray);
        }
      }
      else if (width > 0) {
        initializer.variableWidth(name, width);
      }

      for (ColumnSize columnSize : children.values()) {
        columnSize.buildVectorInitializer(initializer);
      }
    }

  }

  public static ColumnSize getColumn(ValueVector v, String prefix) {
    return new ColumnSize(v, prefix);
  }

  public ColumnSize getColumn(String name) {
    return columnSizes.get(name);
  }

  public static final int MAX_VECTOR_SIZE = ValueVector.MAX_BUFFER_SIZE; // 16 MiB

  // This keeps information for only top level columns. Information for nested
  // columns can be obtained from children of topColumns.
  private Map<String, ColumnSize> columnSizes = CaseInsensitiveMap.newHashMap();

  /**
   * Number of records (rows) in the batch.
   */
  private int rowCount;
  /**
   * Standard row width using Drill meta-data. Note: this information is
   * 100% bogus. Do not use it.
   */
  @Deprecated
  private int stdRowWidth;
  /**
   * Actual batch size summing all buffers used to store data
   * for the batch.
   */
  private long accountedMemorySize;
  /**
   * Actual row width computed by dividing total batch memory by the
   * record count.
   */
  private int grossRowWidth;
  /**
   * Actual row width computed by summing columns. Use this if the
   * vectors are partially full; prevents overestimating row width.
   */
  private int netRowWidth;
  private int netRowWidthCap50;
  private boolean hasSv2;
  private int sv2Size;
  private int avgDensity;

  private Set<BufferLedger> ledgers = Sets.newIdentityHashSet();

  private long netBatchSize;

  /**
   *  Maximum width of a column; used for memory estimation in case of Varchars
   */

  public int maxSize;

  /**
   *  Count the nullable columns; used for memory estimation
   */

  public int nullableCount;


  public RecordBatchSizer(RecordBatch batch) {
    this(batch,
      (batch.getSchema() == null ? null : (batch.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE ?
        batch.getSelectionVector2() : null)));
  }
  /**
   * Create empirical metadata for a record batch given a vector accessible
   * (basically, an iterator over the vectors in the batch.)
   *
   * @param va iterator over the batch's vectors
   */

  public RecordBatchSizer(VectorAccessible va) {
    this(va, null);
  }

  /**
   * Create empirical metadata for a record batch given a vector accessible
   * (basically, an iterator over the vectors in the batch) along with a
   * selection vector for those records. The selection vector is used to
   * pad the estimated row width with the extra two bytes needed per record.
   * The selection vector memory is added to the total memory consumed by
   * this batch.
   *
   * @param va iterator over the batch's vectors
   * @param sv2 selection vector associated with this batch
   */

  public RecordBatchSizer(VectorAccessible va, SelectionVector2 sv2) {
    rowCount = va.getRecordCount();
    for (VectorWrapper<?> vw : va) {
      ColumnSize colSize = measureColumn(vw.getValueVector(), "");
      columnSizes.put(vw.getField().getName(), colSize);

      stdRowWidth += colSize.getStdDataSize();

      int colSizeNet = colSize.getTotalNetSize();
      int colSizePay = vw.getValueVector().getPayloadByteCount(rowCount);

      netBatchSize += colSize.getTotalNetSize();
      maxSize = Math.max(maxSize, colSize.getDataSize());
      if (colSize.metadata.isNullable()) {
        nullableCount++;
      }
      netRowWidth += colSize.getNetSize();
    }

    for (BufferLedger ledger : ledgers) {
      accountedMemorySize += ledger.getAccountedSize();
    }

    if (rowCount > 0) {
      grossRowWidth = safeDivide(accountedMemorySize, rowCount);
    }

    if (sv2 != null) {
      sv2Size = sv2.getBuffer(false).capacity();
      accountedMemorySize += sv2Size;
      hasSv2 = true;
    }

    computeEstimates();
  }

  private void computeEstimates() {
    grossRowWidth = safeDivide(accountedMemorySize, rowCount);
    netRowWidth = safeDivide(netBatchSize, rowCount);
    avgDensity = safeDivide(netBatchSize * 100L, accountedMemorySize);
  }

  public void applySv2() {
    if (hasSv2) {
      return;
    }

    hasSv2 = true;
    sv2Size = BaseAllocator.nextPowerOfTwo(2 * rowCount);
    accountedMemorySize += sv2Size;
    computeEstimates();
  }

  /**
   *  Round up (if needed) to the next power of 2 (only up to 64)
   * @param arg Number to round up (must be < 64)
   * @return power of 2 result
   */
  private int roundUpToPowerOf2(int arg) {
    if ( arg <= 2 ) { return 2; }
    if ( arg <= 4 ) { return 4; }
    if ( arg <= 8 ) { return 8; }
    if ( arg <= 16 ) { return 16; }
    if ( arg <= 32 ) { return 32; }
    return 64;
  }

  private ColumnSize measureColumn(ValueVector v, String prefix) {

    ColumnSize colSize = new ColumnSize(v, prefix);
    // Maps consume no size themselves. However, their contained
    // vectors do consume space, so visit columns recursively.

    /*
    stdRowWidth += colSize.getStdDataSize();
    netBatchSize += colSize.getTotalNetSize();
    maxSize = Math.max(maxSize, colSize.getDataSize());
    if (colSize.metadata.isNullable()) {
      nullableCount++;
    }
    netRowWidth += colSize.getNetSize();
*/
    switch (v.getField().getType().getMinorType()) {
      case MAP:
        expandMap(colSize, (AbstractMapVector) v, prefix + v.getField().getName() + ".");
        break;
      case LIST:
        // complex ListVector cannot be casted to RepeatedListVector.
        // do not expand the list if it is not repeated mode.
        if (v.getField().getDataMode() == DataMode.REPEATED) {
          expandList(colSize, (RepeatedListVector) v, prefix + v.getField().getName() + ".");
        }
        break;
      default:
        v.collectLedgers(ledgers);
    }

    netRowWidthCap50 += ! colSize.isVariableWidth ? colSize.getNetSize() :
        8 /* offset vector */ + roundUpToPowerOf2(Math.min(colSize.getNetSize(),50));
        // above change 8 to 4 after DRILL-5446 is fixed

    return colSize;
  }

  private void expandMap(ColumnSize colSize, AbstractMapVector mapVector, String prefix) {
    for (ValueVector vector : mapVector) {
      colSize.children.put(vector.getField().getName(), measureColumn(vector, prefix));
    }

    // For a repeated map, we need the memory for the offset vector (only).
    // Map elements are recursively expanded above.

    if (mapVector.getField().getDataMode() == DataMode.REPEATED) {
      ((RepeatedMapVector) mapVector).getOffsetVector().collectLedgers(ledgers);
    }
  }

  private void expandList(ColumnSize colSize, RepeatedListVector vector, String prefix) {
    colSize.children.put(vector.getField().getName(),
      measureColumn(vector.getDataVector(), prefix));
   // measureColumn(vector.getDataVector(), prefix);

    // Determine memory for the offset vector (only).

    vector.collectLedgers(ledgers);
  }

  public static int safeDivide(long num, long denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public static int safeDivide(int num, int denom) {
    if (denom == 0) {
      return 0;
    }
    return (int) Math.ceil((double) num / denom);
  }

  public int rowCount() { return rowCount; }
  public int stdRowWidth() { return stdRowWidth; }
  public int grossRowWidth() { return grossRowWidth; }
  public int netRowWidth() { return netRowWidth; }
  public Map<String, ColumnSize> columns() { return columnSizes; }

  /**
   * Compute the "real" width of the row, taking into account each varchar column size
   * (historically capped at 50, and rounded up to power of 2 to match drill buf allocation)
   * and null marking columns.
   * @return "real" width of the row
   */
  public int netRowWidthCap50() { return netRowWidthCap50 + nullableCount; }
  public long actualSize() { return accountedMemorySize; }
  public boolean hasSv2() { return hasSv2; }
  public int avgDensity() { return avgDensity; }
  public long netSize() { return netBatchSize; }
  public int maxAvgColumnSize() { return maxSize / rowCount; }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Actual batch schema & sizes {\n");
    for (ColumnSize colSize : columnSizes.values()) {
      buf.append("  ");
      buf.append(colSize.toString());
      buf.append("\n");
    }
    buf.append( "  Records: " );
    buf.append(rowCount);
    buf.append(", Total size: ");
    buf.append(accountedMemorySize);
    buf.append(", Data size: ");
    buf.append(netBatchSize);
    buf.append(", Gross row width: ");
    buf.append(grossRowWidth);
    buf.append(", Net row width: ");
    buf.append(netRowWidth);
    buf.append(", Density: ");
    buf.append(avgDensity);
    buf.append("%}");
    return buf.toString();
  }

  /**
   * The column size information gathered here represents empirically-derived
   * schema metadata. Use that metadata to create an instance of a class that
   * allocates memory for new vectors based on the observed size information.
   * The caller provides the row count; the size information here provides
   * column widths and the number of elements in each array.
   */

  public VectorInitializer buildVectorInitializer() {
    VectorInitializer initializer = new VectorInitializer();
    for (ColumnSize colSize : columnSizes.values()) {
      colSize.buildVectorInitializer(initializer);
    }
    return initializer;
  }
}
