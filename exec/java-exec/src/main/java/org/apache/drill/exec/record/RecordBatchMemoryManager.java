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

import com.google.common.base.Preconditions;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.RepeatedListVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

import java.util.Map;

public class RecordBatchMemoryManager {
  protected static final int OFFSET_VECTOR_WIDTH = 4;
  protected static final int MAX_NUM_ROWS = ValueVector.MAX_ROW_COUNT;
  protected static final int MIN_NUM_ROWS = 1;
  protected static final int DEFAULT_INPUT_INDEX = 0;
  private int outputRowCount = MAX_NUM_ROWS;
  private int outgoingRowWidth;
  private RecordBatchSizer[] sizer;
  private BatchStats[] inputBatchStats;
  private BatchStats outputBatchStats;

  // By default, we expect one input batch stream and one output batch stream.
  // Some operators can get multiple input batch streams i.e. for example
  // joins get 2 batches (left and right). Merge Receiver can get more than 2.
  private int numInputs = 1;

  private class BatchStats {
    /**
     * operator metric stats
     */
    private long numBatches;
    private long sumBatchSizes;
    private long totalRecords;

    Map<String, ColumnStats> columnStats = CaseInsensitiveMap.newHashMap();;

    public void allocateColumnStats(Map<String, RecordBatchSizer.ColumnSize> columnSizeMap,
                                    Map<String, ColumnStats> columnStatsMap) {

      if (columnSizeMap == null) {
        return;
      }

      for (Map.Entry<String, RecordBatchSizer.ColumnSize> columnSizeEntry : columnSizeMap.entrySet()) {
        columnStatsMap.put(columnSizeEntry.getKey(), new ColumnStats(columnSizeEntry.getKey()));
        allocateColumnStats(columnSizeEntry.getValue().getChildren(), columnStatsMap.get(columnSizeEntry.getKey()).children);
      }

    }

    public BatchStats(RecordBatchSizer sizer) {
      allocateColumnStats(sizer.columns(), columnStats);
    }

    public BatchStats() {

    }

    private class ColumnStats {
      private String name;
      private long sumColumnSizes;

      private long totalElements;

      private Map<String, ColumnStats> children = CaseInsensitiveMap.newHashMap();

      public void updateStats(RecordBatchSizer.ColumnSize columnSize) {
        sumColumnSizes += columnSize.getTotalNetSize();
        totalElements += columnSize.getElementCount();

        for (Map.Entry<String, RecordBatchSizer.ColumnSize> entry : columnSize.getChildren().entrySet()) {
          children.get(entry.getKey()).updateStats(entry.getValue());
        }

      }

      public ColumnStats(String name) {
        this.name = name;
      }

      public float avgcolumnSize() {
        return RecordBatchSizer.safeDivide(sumColumnSizes, totalRecords);
      }

      public float avgCardinality() {
        return RecordBatchSizer.safeDivide(totalElements, totalRecords);
      }

      private void allocateMap(AbstractMapVector map, int recordCount) {
        if (map instanceof RepeatedMapVector) {
          ((RepeatedMapVector) map).allocateOffsetsNew(recordCount);
          recordCount *= avgCardinality();
        }

        for (ValueVector vector : map) {
          children.get(vector.getField().getName()).allocateVector(vector, recordCount);
        }
      }

      private void allocateRepeatedList(RepeatedListVector vector, int recordCount) {
        vector.allocateOffsetsNew(recordCount);
        recordCount *= avgCardinality();
        ColumnStats child = children.get(vector.getField().getName());
        if (vector.getDataVector() != null) {
          child.allocateVector(vector.getDataVector(), recordCount);
        }
      }

      public void allocateVector(ValueVector vector, int recordCount) {
        if (vector instanceof AbstractMapVector) {
          allocateMap((AbstractMapVector) vector, recordCount);
          return;
        }

        if (vector instanceof RepeatedListVector) {
          allocateRepeatedList((RepeatedListVector) vector, recordCount);
          return;
        }

        AllocationHelper.allocate(vector, recordCount, (int) avgcolumnSize(), avgCardinality());
      }

    }

    public void updateColumnStats(RecordBatchSizer sizer) {
      for (Map.Entry<String, RecordBatchSizer.ColumnSize> entry : sizer.columns().entrySet()) {
        columnStats.get(entry.getKey()).updateStats(entry.getValue());
      }
    }

    public long getNumBatches() {
      return numBatches;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getAvgBatchSize() {
      return RecordBatchSizer.safeDivide(sumBatchSizes, numBatches);
    }

    public long getAvgRowWidth() {
      return RecordBatchSizer.safeDivide(sumBatchSizes, totalRecords);
    }

    public void incNumBatches() {
      ++numBatches;
    }

    public void incSumBatchSizes(long batchSize) {
      sumBatchSizes += batchSize;
    }

    public void incTotalRecords(long numRecords) {
      totalRecords += numRecords;
    }

  }

  public long getNumOutgoingBatches() {
    return outputBatchStats.getNumBatches();
  }

  public long getTotalOutputRecords() {
    return outputBatchStats.getTotalRecords();
  }

  public long getAvgOutputBatchSize() {
    return outputBatchStats.getAvgBatchSize();
  }

  public long getAvgOutputRowWidth() {
    return outputBatchStats.getAvgRowWidth();
  }

  public long getNumIncomingBatches() {
    return inputBatchStats[DEFAULT_INPUT_INDEX].getNumBatches();
  }

  public long getAvgInputBatchSize() {
    return inputBatchStats[DEFAULT_INPUT_INDEX].getAvgBatchSize();
  }

  public long getAvgInputRowWidth() {
    return inputBatchStats[DEFAULT_INPUT_INDEX].getAvgRowWidth();
  }

  public long getTotalInputRecords() {
    return inputBatchStats[DEFAULT_INPUT_INDEX].getTotalRecords();
  }

  public long getNumIncomingBatches(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getNumBatches();
  }

  public long getAvgInputBatchSize(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getAvgBatchSize();
  }

  public long getAvgInputRowWidth(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getAvgRowWidth();
  }

  public long getTotalInputRecords(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return inputBatchStats[index] == null ? 0 : inputBatchStats[index].getTotalRecords();
  }

  public RecordBatchMemoryManager(int numInputs) {
    this.numInputs = numInputs;
    sizer = new RecordBatchSizer[numInputs];
    inputBatchStats = new BatchStats[numInputs];
    outputBatchStats = new BatchStats();
  }

  public RecordBatchMemoryManager() {
    sizer = new RecordBatchSizer[numInputs];
    inputBatchStats = new BatchStats[numInputs];
    outputBatchStats = new BatchStats();
  }

  public void update(int inputIndex) {};

  public void update() {};

  public int getOutputRowCount() {
    return outputRowCount;
  }

  /**
   * Given batchSize and rowWidth, this will set output rowCount taking into account
   * the min and max that is allowed.
   */
  public void setOutputRowCount(int targetBatchSize, int rowWidth) {
    this.outputRowCount = adjustOutputRowCount(RecordBatchSizer.safeDivide(targetBatchSize, rowWidth));
  }

  public void setOutputRowCount(int outputRowCount) {
    this.outputRowCount = outputRowCount;
  }

  /**
   * This will adjust rowCount taking into account the min and max that is allowed.
   * We will round down to nearest power of two - 1 for better memory utilization.
   * -1 is done for adjusting accounting for offset vectors.
   */
  public static int adjustOutputRowCount(int rowCount) {
    return (Math.min(MAX_NUM_ROWS, Math.max(Integer.highestOneBit(rowCount) - 1, MIN_NUM_ROWS)));
  }

  public void setOutgoingRowWidth(int outgoingRowWidth) {
    this.outgoingRowWidth = outgoingRowWidth;
  }

  public int getOutgoingRowWidth() {
    return outgoingRowWidth;
  }

  public void setRecordBatchSizer(int index, RecordBatchSizer sizer) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    this.sizer[index] = sizer;
    if (inputBatchStats[index] == null) {
      inputBatchStats[index] = new BatchStats(sizer);
    }
    updateIncomingStats(index);
  }

  public void setRecordBatchSizer(RecordBatchSizer sizer) {
    this.sizer[DEFAULT_INPUT_INDEX] = sizer;
    if (inputBatchStats[DEFAULT_INPUT_INDEX] == null) {
      inputBatchStats[DEFAULT_INPUT_INDEX] = new BatchStats(sizer);
    }
  }

  public RecordBatchSizer getRecordBatchSizer(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return sizer[index];
  }

  public RecordBatchSizer getRecordBatchSizer() {
    return sizer[DEFAULT_INPUT_INDEX];
  }

  public RecordBatchSizer.ColumnSize getColumnSize(int index, String name) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    return sizer[index].getColumn(name);
  }

 // public RecordBatchSizer.ColumnSize getColumnSize(String name) {
 //   return sizer[DEFAULT_INPUT_INDEX].getColumn(name);
 // }

  public void updateIncomingStats(int index) {
    Preconditions.checkArgument(index >= 0 && index < numInputs);
    Preconditions.checkArgument(inputBatchStats[index] != null);
    inputBatchStats[index].incNumBatches();
    inputBatchStats[index].incSumBatchSizes(sizer[index].netSize());
    inputBatchStats[index].incTotalRecords(sizer[index].rowCount());

    inputBatchStats[index].updateColumnStats(sizer[index]);
  }

  public void updateIncomingStats() {
    inputBatchStats[DEFAULT_INPUT_INDEX].incNumBatches();
    inputBatchStats[DEFAULT_INPUT_INDEX].incSumBatchSizes(sizer[DEFAULT_INPUT_INDEX].netSize());
    inputBatchStats[DEFAULT_INPUT_INDEX].incTotalRecords(sizer[DEFAULT_INPUT_INDEX].rowCount());
    inputBatchStats[DEFAULT_INPUT_INDEX].updateColumnStats(sizer[DEFAULT_INPUT_INDEX]);
  }

  public void updateOutgoingStats(int outputRecords) {
    outputBatchStats.incNumBatches();
    outputBatchStats.incTotalRecords(outputRecords);
    outputBatchStats.incSumBatchSizes(outgoingRowWidth * outputRecords);
  }

  public void allocateVectors(VectorContainer container, int recordCount) {
    for (VectorWrapper w : container) {
      for (int index = 0; index < numInputs; index++) {
        BatchStats.ColumnStats colStats = inputBatchStats[index].columnStats.get(w.getField().getName());
        if (colStats == null) {
          continue;
        }
        colStats.allocateVector(w.getValueVector(), recordCount);
        break;
      }
    }
  }

  public RecordBatchSizer.ColumnSize getColumnSize(String name) {

    for (int index = 0; index < numInputs; index++) {
      if (sizer[index] == null || sizer[index].getColumn(name) == null) {
        continue;
      }
      return sizer[index].getColumn(name);
    }

    return null;
  }

}
