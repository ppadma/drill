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
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchMemoryManager;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.UInt1Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;


import java.util.HashMap;
import java.util.Map;

public class ProjectMemoryManager extends RecordBatchMemoryManager {

    RecordBatch incomingBatch = null;
    ProjectRecordBatch outgoingBatch = null;

    int rowWidth = 0;
    Map<String, ColumnSizeInfo> outputColumnSizes;
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

    class ColumnSizeInfo {
        MaterializedField materializedField;
        OutputWidthExpression outputExpression;
        int width;
        WidthType widthType;
        OutputColumnType outputColumnType;

        ColumnSizeInfo (ValueVector vv,
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

    public static void Unimplemented(String message) {
        throw new IllegalStateException(message);
    }

    public ProjectMemoryManager(int configuredOutputSize) {
        super(configuredOutputSize);
        outputColumnSizes = new HashMap<>();
        this.outgoingBatch = outgoingBatch;
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

    static int getWidthOfFixedWidthType(MajorType majorType) {
        //KM_TBD: Reoeated types ?
        //final int OFFSET_VECTOR_WIDTH = UInt4Vector.VALUE_WIDTH;
        final int BIT_VECTOR_WIDTH = UInt1Vector.VALUE_WIDTH;
        DataMode mode = majorType.getMode();
        final boolean isOptional = (mode == DataMode.OPTIONAL);
        final boolean isRepeated = (mode == DataMode.REPEATED);
        MinorType minorType = majorType.getMinorType();
        final boolean isVariableWidth  = (minorType == MinorType.VARCHAR || minorType == MinorType.VAR16CHAR
                                                                    || minorType == MinorType.VARBINARY);

        assert !isVariableWidth && !isRepeated;
        int stdDataSize = TypeHelper.getSize(majorType);

        if (isOptional) { stdDataSize += BIT_VECTOR_WIDTH; }

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
            ColumnSizeInfo columnSizeInfo;
            if(outputColumnType == OutputColumnType.TRANSFER) {
                VarLenReadExpr readExpr = new VarLenReadExpr(vv.getField().getName());
                columnSizeInfo = new ColumnSizeInfo(vv, readExpr, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the RecordBatchSizer
            } else {
                OutputWidthVisitorState state = new OutputWidthVisitorState(this, outputColumnType);
                OutputWidthExpression outputWidthExpression = logicalExpression.accept(new OutputWidthVisitor(), state);
                columnSizeInfo = new ColumnSizeInfo(vv, outputWidthExpression, outputColumnType,
                        WidthType.VARIABLE, -1); //fieldWidth has to be obtained from the RecordBatchSizer
            }
            outputColumnSizes.put(columnSizeInfo.getName(), columnSizeInfo);
        }
    }


    void addFixedWidthField(TypedFieldId fieldId, OutputColumnType outputColumnType) {
        ValueVector vv = getOutgoingValueVector(fieldId);
        addFixedWidthField(vv, outputColumnType);
    }


    void addFixedWidthField(ValueVector vv, OutputColumnType outputColumnType) {
        assert isFixedWidth(vv);
        int fixedFieldWidth = getWidthOfFixedWidthType(vv);
        ColumnSizeInfo columnSizeInfo = new ColumnSizeInfo(vv, null, outputColumnType, WidthType.FIXED,
                                                           fixedFieldWidth);
        outputColumnSizes.put(columnSizeInfo.getName(), columnSizeInfo);
        //rowWidth += columnSizeInfo.width;
    }

    @Override
    public void update() {
        RecordBatchSizer batchSizer = new RecordBatchSizer(incomingBatch);
        setRecordBatchSizer(batchSizer);
        rowWidth = 0;
        for (String expr : outputColumnSizes.keySet()) {
            ColumnSizeInfo columnSizeInfo = outputColumnSizes.get(expr);
            int width = -1;
            if (columnSizeInfo.isFixedWidth()) {
                width = columnSizeInfo.getWidth();
            } else {
                OutputWidthExpression savedWidthExpr = columnSizeInfo.getOutputExpression();
                OutputColumnType columnType = columnSizeInfo.getOutputColumnType();
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
