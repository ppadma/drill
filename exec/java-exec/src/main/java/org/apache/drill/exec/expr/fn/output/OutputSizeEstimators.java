/**
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

package org.apache.drill.exec.expr.fn.output;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchSizer;
import org.apache.drill.exec.record.TypedFieldId;

import java.util.List;

/**
 * Return type calculation implementation for functions with return type set as
 * {@link org.apache.drill.exec.expr.annotations.FunctionTemplate.ReturnType#CONCAT}.
 */

public class OutputSizeEstimators {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OutputSizeEstimators.class);

    static int getOutputSize(LogicalExpression expr, RecordBatchSizer recordBatchSizer, RecordBatch recordBatch) {
        //KM_TBD: Move functionality as instance methods in the corresponding expr classes
        if (expr.getMajorType().hasPrecision()) {
            return expr.getMajorType().getPrecision();
        } else if (expr instanceof DrillFuncHolderExpr) {
            DrillFuncHolderExpr funcHolder = ((DrillFuncHolderExpr)expr);
            return funcHolder.getHolder().getOutputSizeEstimate(funcHolder.args, recordBatchSizer, recordBatch);
        } else if (expr instanceof ValueVectorReadExpression) {
            TypedFieldId fid = ((ValueVectorReadExpression)expr).getFieldId();
            String name = recordBatch.getSchema().getColumn(fid.getFieldIds()[0]).getName();
            return recordBatchSizer.getColumn(name).getNetSizePerEntry();
        } else if (expr instanceof ValueVectorWriteExpression) {
            return getOutputSize(((ValueVectorWriteExpression)expr).getChild(), recordBatchSizer, recordBatch);
        }
        return -1;
    }

    public static class ConcatOutputSizeEstimator implements OutputSizeEstimator {

        public static final ConcatOutputSizeEstimator INSTANCE = new ConcatOutputSizeEstimator();

        /**
         * Defines function's output size estimate, which is caluclated as
         * sum of input sizes
         * If calculated size is greater than {@link Types#MAX_VARCHAR_LENGTH},
         * it is replaced with {@link Types#MAX_VARCHAR_LENGTH}.
         *
         * @param logicalExpressions logical expressions
         * @return return type
         */
        @Override
        public int getEstimatedOutputSize(List<LogicalExpression> logicalExpressions, RecordBatchSizer recordBatchSizer,
                                          RecordBatch recordBatch) {
            int outputSize = 0;
            for (LogicalExpression expr : logicalExpressions) {
                outputSize += getOutputSize(expr, recordBatchSizer, recordBatch);
            }
            if (outputSize > Types.MAX_VARCHAR_LENGTH || outputSize < 0 /*overflow*/) {
                logger.warn("Output size for expressions is too large, setting to MAX_VARCHAR_LENGTH");
                outputSize = Types.MAX_VARCHAR_LENGTH;
            }
            return outputSize;
            //return builder.setPrecision(outputSize > Types.MAX_VARCHAR_LENGTH ? Types.MAX_VARCHAR_LENGTH : totalPrecision).build();
        }
    }

    public static class CloneOutputSizeEstimator implements OutputSizeEstimator {

        public static final CloneOutputSizeEstimator INSTANCE = new CloneOutputSizeEstimator();

        /**
         * Defines function's output size estimate, which is caluclated as
         * sum of input sizes
         * If calculated size is greater than {@link Types#MAX_VARCHAR_LENGTH},
         * it is replaced with {@link Types#MAX_VARCHAR_LENGTH}.
         *
         * @param logicalExpressions logical expressions
         * @return return type
         */
        @Override
        public int getEstimatedOutputSize(List<LogicalExpression> logicalExpressions, RecordBatchSizer recordBatchSizer,
                                          RecordBatch recordBatch) {
            int outputSize = 0;
            if (logicalExpressions.size() != 1) {
                throw new IllegalArgumentException();
            }
            outputSize = getOutputSize(logicalExpressions.get(0), recordBatchSizer, recordBatch);
            if (outputSize > Types.MAX_VARCHAR_LENGTH || outputSize < 0 /*overflow*/) {
                logger.warn("Output size for expressions is too large, setting to MAX_VARCHAR_LENGTH");
                outputSize = Types.MAX_VARCHAR_LENGTH;
            }
            return outputSize;
            //return builder.setPrecision(outputSize > Types.MAX_VARCHAR_LENGTH ? Types.MAX_VARCHAR_LENGTH : totalPrecision).build();
        }
    }

}
