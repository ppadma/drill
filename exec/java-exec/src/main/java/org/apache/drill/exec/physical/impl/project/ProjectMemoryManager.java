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
import org.apache.drill.exec.expr.AbstractExecExprVisitor;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;
import org.apache.drill.exec.record.MaterializedField;
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

public class ProjectMemoryManager extends RecordBatchMemoryManager {

    RecordBatch incomingBatch = null;
    ProjectRecordBatch outgoingBatch = null;

    int rowWidth = 0;
    Map<String, ColumnWidthInfo> outputColumnSizes;
    int variableWidthColumnCount = 0;
    int fixedWidthColumnCount = 0;

    enum WidthType {
        FIXED,
        VARIABLE
    }

    enum OutputColumnType {
        TRANSFER,
        NEW
    }

    class ColumnWidthInfo {
        MaterializedField materializedField;
        OutputWidthExpression outputExpression;
        int width;
        WidthType widthType;
        OutputColumnType outputColumnType;

        ColumnWidthInfo(ValueVector vv,
                        OutputWidthExpression outputWidthExpression,
                        OutputColumnType outputColumnType,
                        WidthType widthType,
                        int fieldWidth) {
            this.materializedField = vv.getField();
            this.outputExpression = outputWidthExpression;
            this.width = fieldWidth;
            this.outputColumnType = outputColumnType;
            this.widthType = widthType;
        }

        public OutputWidthExpression getOutputExpression() { return outputExpression; }

        public OutputColumnType getOutputColumnType() { return outputColumnType; }

        boolean isFixedWidth() { return widthType == WidthType.FIXED; }

        public int getWidth() { return width; }

        public String getName() { return materializedField.getName(); }
    }

    public void setIncomingBatch(RecordBatch recordBatch) {
        incomingBatch = recordBatch;
    }

    public void setOutgoingBatch(ProjectRecordBatch outgoingBatch) {
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

    public ValueVector getIncomingValueVector(TypedFieldId fieldId) {
        Class<?> clazz = fieldId.getIntermediateClass();
        int[] fieldIds = fieldId.getFieldIds();
        return incomingBatch.getValueAccessorById(clazz, fieldIds).getValueVector();
    }

    static boolean isFixedWidth(ValueVector vv) {
       return (vv instanceof FixedWidthVector);
    }

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

        int stdDataSize = TypeHelper.getSize(majorType);

        if (isOptional) { stdDataSize += BIT_VECTOR_WIDTH; }

        if (isRepeated) { stdDataSize = (stdDataSize * STD_REPETITION_FACTOR) + OFFSET_VECTOR_WIDTH; }

        return stdDataSize;
    }

    /**
     * Valid only for fixed width vectors
     * @param vv
     * @return
     */
    static int getWidthOfFixedWidthType(ValueVector vv) {
        assert isFixedWidth(vv);
        return vv.getPayloadByteCount(1);
    }

    void addTransferField(ValueVector vvOut) {
        addField(vvOut, null, OutputColumnType.TRANSFER);
    }

    void addNewField(ValueVector vv, LogicalExpression logicalExpression) {
        addField(vv, logicalExpression, OutputColumnType.NEW);
    }

    void addField(ValueVector vv, LogicalExpression logicalExpression, OutputColumnType outputColumnType) {
        if(isFixedWidth(vv)) {
            fixedWidthColumnCount++;
            addFixedWidthField(vv, outputColumnType);
        } else {
            variableWidthColumnCount++;
            ColumnWidthInfo columnWidthInfo;
            if(outputColumnType == OutputColumnType.TRANSFER) {
                VarLenReadExpr readExpr = new VarLenReadExpr(vv.getField().getName());
                columnWidthInfo = new ColumnWidthInfo(vv, readExpr, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the RecordBatchSizer
            } else if (isComplex(vv.getField().getType())) {
                // Complex types are not yet supported. Treat complex types as 50 bytes wide
                columnWidthInfo = new ColumnWidthInfo(vv, null, outputColumnType, WidthType.FIXED, 50);
            } else {
                OutputWidthVisitorState state = new OutputWidthVisitorState(this, outputColumnType);
                OutputWidthExpression outputWidthExpression = logicalExpression.accept(new OutputWidthVisitor(), state);
                columnWidthInfo = new ColumnWidthInfo(vv, outputWidthExpression, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the outputWidthExpression
            }
            outputColumnSizes.put(columnWidthInfo.getName(), columnWidthInfo);
        }
    }


    void addFixedWidthField(TypedFieldId fieldId, OutputColumnType outputColumnType) {
        ValueVector vv = getOutgoingValueVector(fieldId);
        addFixedWidthField(vv, outputColumnType);
    }


    void addFixedWidthField(ValueVector vv, OutputColumnType outputColumnType) {
        assert isFixedWidth(vv);
        int fixedFieldWidth = getWidthOfFixedWidthType(vv);
        ColumnWidthInfo columnWidthInfo = new ColumnWidthInfo(vv, null, outputColumnType, WidthType.FIXED,
                                                           fixedFieldWidth);
        outputColumnSizes.put(columnWidthInfo.getName(), columnWidthInfo);
        //rowWidth += columnWidthInfo.width;
    }

    @Override
    public void update() {
        RecordBatchSizer batchSizer = new RecordBatchSizer(incomingBatch);
        setRecordBatchSizer(batchSizer);
        rowWidth = 0;
        for (String expr : outputColumnSizes.keySet()) {
            ColumnWidthInfo columnWidthInfo = outputColumnSizes.get(expr);
            int width = -1;
            if (columnWidthInfo.isFixedWidth()) {
                width = columnWidthInfo.getWidth();
            } else {
                OutputWidthExpression savedWidthExpr = columnWidthInfo.getOutputExpression();
                OutputColumnType columnType = columnWidthInfo.getOutputColumnType();
                OutputWidthVisitorState state = new OutputWidthVisitorState(this, columnType);
                OutputWidthExpression reducedExpr = savedWidthExpr.accept(new OutputWidthVisitor(), state);
                assert reducedExpr instanceof FixedLenExpr;
                width = ((FixedLenExpr)reducedExpr).getWidth();
            }
            assert width > 0;
            rowWidth += width;
        }

        setOutputRowCount(getOutputBatchSize(), rowWidth);
        int outPutRowCount = Math.min(getOutputRowCount(), batchSizer.rowCount());
        //KM_TBD: Change this once variable width is implemented
        if (rowWidth == 0 || variableWidthColumnCount != 0) {
            outPutRowCount = batchSizer.rowCount();
        }
        setOutputRowCount(outPutRowCount);
    }
}
