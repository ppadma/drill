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
package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;

import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.exec.vector.AllocationHelper.STD_REPETITION_FACTOR;

/**
 *
 * ProjectMemoryManager(PMM) is used to estimate the size of rows produced by ProjectRecordBatch.
 * The PMM works as follows:
 *
 * Setup phase: As and when ProjectRecordBatch creates or transfers a field, it registers the field with PMM.
 * If the field is a variable width field, PMM records the expression that produces the variable
 * width field. The expression is a tree of LogicalExpressions. The PMM walks this tree of LogicalExpressions
 * to produce a tree of OutputWidthExpressions. The widths of Fixed width fields are just accumulated into a single
 * total. Note: The PMM, currently, cannot handle new complex fields, it just uses a hard-coded estimate for such fields.
 *
 *
 * Execution phase: Just before a batch is processed by Project, the PMM walks the tree of OutputWidthExpressions
 * and conversts them to FixedWidthExpressions. It uses the RecordBatchSizer and the function annotations to do this conversion.
 * See OutputWidthVisitor for details.
 */
public class ProjectMemoryManager extends RecordBatchMemoryManager {

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ProjectMemoryManager.class);

    public RecordBatch getIncomingBatch() {
        return incomingBatch;
    }

    RecordBatch incomingBatch = null;
    ProjectRecordBatch outgoingBatch = null;

    int rowWidth = 0;
    Map<String, ColumnWidthInfo> outputColumnSizes;
    // Number of variable width columns in the batch
    int variableWidthColumnCount = 0;
    // Number of fixed width columns in the batch
    int fixedWidthColumnCount = 0;
    // Number of complex columns in the batch
    int complexColumnsCount = 0;


    // Holds sum of all fixed width column widths
    int totalFixedWidthColumnWidth = 0;
    // Holds sum of all complex column widths
    // Currently, this is just a guess
    int totalComplexColumnWidth = 0;

    enum WidthType {
        FIXED,
        VARIABLE
    }

    enum OutputColumnType {
        TRANSFER,
        NEW
    }

    class ColumnWidthInfo {
        //MaterializedField materializedField;
        OutputWidthExpression outputExpression;
        int width;
        WidthType widthType;
        OutputColumnType outputColumnType;
        String name;

        ColumnWidthInfo(ValueVector vv,
                        OutputWidthExpression outputWidthExpression,
                        OutputColumnType outputColumnType,
                        WidthType widthType,
                        int fieldWidth) {
            this.outputExpression = outputWidthExpression;
            this.width = fieldWidth;
            this.outputColumnType = outputColumnType;
            this.widthType = widthType;
            String columnName = vv.getField().getName();
            this.name = columnName;
        }

        public OutputWidthExpression getOutputExpression() { return outputExpression; }

        public OutputColumnType getOutputColumnType() { return outputColumnType; }

        boolean isFixedWidth() { return widthType == WidthType.FIXED; }

        public int getWidth() { return width; }

        public String getName() { return name; }
    }

    void ShoulgNotReachHere() {
        throw new IllegalStateException();
    }

    private void setIncomingBatch(RecordBatch recordBatch) {
        incomingBatch = recordBatch;
    }

    private void setOutgoingBatch(ProjectRecordBatch outgoingBatch) {
        this.outgoingBatch = outgoingBatch;
    }

    public ProjectMemoryManager(int configuredOutputSize) {
        super(configuredOutputSize);
        outputColumnSizes = new HashMap<>();
    }

    public boolean isComplex(MajorType majorType) {
        MinorType minorType = majorType.getMinorType();
        return minorType == MinorType.MAP || minorType == MinorType.UNION || minorType == MinorType.LIST;
    }

    boolean isFixedWidth(TypedFieldId fieldId) {
        ValueVector vv = getOutgoingValueVector(fieldId);
        return (vv instanceof FixedWidthVector);
    }

    public ValueVector getOutgoingValueVector(TypedFieldId fieldId) {
        Class<?> clazz = fieldId.getIntermediateClass();
        int[] fieldIds = fieldId.getFieldIds();
        return outgoingBatch.getValueAccessorById(clazz, fieldIds).getValueVector();
    }

    static boolean isFixedWidth(ValueVector vv) {  return (vv instanceof FixedWidthVector); }

    static final int BIT_VECTOR_WIDTH = UInt1Vector.VALUE_WIDTH;
    static final int OFFSET_VECTOR_WIDTH = UInt4Vector.VALUE_WIDTH;

    static int getWidthOfFixedWidthType(MajorType majorType) {
        DataMode mode = majorType.getMode();
        MinorType minorType = majorType.getMinorType();
        final boolean isVariableWidth  = (minorType == MinorType.VARCHAR || minorType == MinorType.VAR16CHAR
                || minorType == MinorType.VARBINARY);


        if (isVariableWidth) {
            throw new IllegalArgumentException("getWidthOfFixedWidthType() cannot handle variable width types");
        }

        final boolean isOptional = (mode == DataMode.OPTIONAL);
        final boolean isRepeated = (mode == DataMode.REPEATED);

        int stdDataSize = 0;

        if (isOptional) { stdDataSize += BIT_VECTOR_WIDTH; }

        if (minorType != MinorType.NULL) {
            stdDataSize += TypeHelper.getSize(majorType);
        }

        if (isRepeated) {
            stdDataSize = (stdDataSize * STD_REPETITION_FACTOR) + OFFSET_VECTOR_WIDTH;
        }

        return stdDataSize;
    }

    static int getWidthOfFixedWidthType(ValueVector vv) {
        assert isFixedWidth(vv);
        return vv.getPayloadByteCount(1);
    }

    void addTransferField(ValueVector vvOut, String path) {
        addField(vvOut, null, OutputColumnType.TRANSFER, path);
    }

    void addNewField(ValueVector vv, LogicalExpression logicalExpression) {
        addField(vv, logicalExpression, OutputColumnType.NEW, null);
    }

    void addField(ValueVector vv, LogicalExpression logicalExpression, OutputColumnType outputColumnType, String path) {
        if(isFixedWidth(vv)) {
            addFixedWidthField(vv);
        } else {
            variableWidthColumnCount++;
            ColumnWidthInfo columnWidthInfo;
            //Variable width transfers
            if(outputColumnType == OutputColumnType.TRANSFER) {
                String columnName = path;
                VarLenReadExpr readExpr = new VarLenReadExpr(columnName);
                columnWidthInfo = new ColumnWidthInfo(vv, readExpr, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the RecordBatchSizer
            } else if (isComplex(vv.getField().getType())) {
                addComplexField(vv);
                return;
            } else {
                // Walk the tree of LogicalExpressions to get a tree of OutputWidthExpressions
                OutputWidthVisitorState state = new OutputWidthVisitorState(this, outputColumnType);
                OutputWidthExpression outputWidthExpression = logicalExpression.accept(new OutputWidthVisitor(), state);
                columnWidthInfo = new ColumnWidthInfo(vv, outputWidthExpression, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the OutputWidthExpression
            }
            outputColumnSizes.put(columnWidthInfo.getName(), columnWidthInfo);
        }
    }

    void addComplexField(ValueVector vv) {
        //Complex types are not yet supported. Just use a guess for the size
        assert vv == null || isComplex(vv.getField().getType());
        complexColumnsCount++;
        // just a guess
        totalComplexColumnWidth +=  OutputSizeEstimateConstants.COMPLEX_FIELD_ESTIMATE;
    }

    void addFixedWidthField(ValueVector vv) {
        assert isFixedWidth(vv);
        fixedWidthColumnCount++;
        int fixedFieldWidth = getWidthOfFixedWidthType(vv);
        totalFixedWidthColumnWidth += fixedFieldWidth;
    }

    public void init(RecordBatch incomingBatch, ProjectRecordBatch outgoingBatch) {
        setIncomingBatch(incomingBatch);
        setOutgoingBatch(outgoingBatch);
        reset();
    }

    private void reset() {
        rowWidth = 0;
        totalFixedWidthColumnWidth = 0;
        totalComplexColumnWidth = 0;

        fixedWidthColumnCount = 0;
        complexColumnsCount = 0;
    }

    @Override
    public void update() {
        long updateStartTime = System.currentTimeMillis();
        RecordBatchSizer batchSizer = new RecordBatchSizer(incomingBatch);
        long batchSizerEndTime = System.currentTimeMillis();

        setRecordBatchSizer(batchSizer);
        rowWidth = 0;
        int totalVariableColumnWidth = 0;
        for (String expr : outputColumnSizes.keySet()) {
            ColumnWidthInfo columnWidthInfo = outputColumnSizes.get(expr);
            int width = -1;
            if (columnWidthInfo.isFixedWidth()) {
                // fixed width columns are accumulated in totalFixedWidthColumnWidth
                ShoulgNotReachHere();
            } else {
                //Walk the tree of OutputWidthExpressions to get a FixedLenExpr
                //As the tree is walked, the RecordBatchSizer and function annotations
                //are looked-up to come up with the final FixedLenExpr
                OutputWidthExpression savedWidthExpr = columnWidthInfo.getOutputExpression();
                OutputColumnType columnType = columnWidthInfo.getOutputColumnType();
                OutputWidthVisitorState state = new OutputWidthVisitorState(this, columnType);
                OutputWidthExpression reducedExpr = savedWidthExpr.accept(new OutputWidthVisitor(), state);
                assert reducedExpr instanceof FixedLenExpr;
                width = ((FixedLenExpr)reducedExpr).getWidth();
                assert width >= 0;
            }
            totalVariableColumnWidth += width;
        }
        rowWidth += totalFixedWidthColumnWidth;
        rowWidth += totalComplexColumnWidth;
        rowWidth += totalVariableColumnWidth;
        int outPutRowCount;
        if (rowWidth != 0) {
            setOutputRowCount(getOutputBatchSize(), rowWidth);
            outPutRowCount = Math.min(getOutputRowCount(), batchSizer.rowCount());
        } else {
            // if rowWidth == 0 then the memory manager does
            // not have sufficient information to size the batch
            // let the entire batch pass through.
            // If incoming rc == 0, all RB Sizer lookups will have
            // 0 width and so total width can be 0
            outPutRowCount = incomingBatch.getRecordCount();
        }
        setOutputRowCount(outPutRowCount);
        long updateEndTime = System.currentTimeMillis();
        if (logger.isTraceEnabled()) {
            logger.trace("update() : Output RC " + outPutRowCount + ", BatchSizer RC " + batchSizer.rowCount()
                    + ", incoming RC " + incomingBatch.getRecordCount() + " width " + rowWidth
                    + ", total fixed width " + totalFixedWidthColumnWidth
                    + ", total variable width " + totalVariableColumnWidth
                    + ", total complex width " + totalComplexColumnWidth
                    + ", batchSizer time " + (batchSizerEndTime - updateStartTime) + " ms"
                    + ", update time " + (updateEndTime - updateStartTime) + " ms"
                    + ", manager " + this
                    + ", incoming " + incomingBatch);
        }

        logger.debug("BATCH_STATS, incoming: {}", getRecordBatchSizer());
        updateIncomingStats();
    }
}
