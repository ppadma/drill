package org.apache.drill.exec.physical.impl.project;

import org.apache.drill.common.expression.FunctionHolderExpression;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.AbstractExecExprVisitor;
import org.apache.drill.exec.expr.DrillFuncHolderExpr;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.output.OutputSizeEstimator;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FunctionCallExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.vector.ValueVector;

import java.util.ArrayList;

public class OutputWidthVisitor extends AbstractExecExprVisitor<OutputWidthExpression, OutputWidthVisitorState,
        RuntimeException> {

    @Override
    public OutputWidthExpression visitFunctionHolderExpression(FunctionHolderExpression holderExpr,
                                                               OutputWidthVisitorState state) throws RuntimeException {
        OutputWidthExpression fixedWidth = getFixedLenExpr(holderExpr.getMajorType());
        if (fixedWidth != null) { return fixedWidth; }
        //KM_TBD Handling for HiveFunctionHolder
        OutputSizeEstimator estimator = ((DrillFuncHolderExpr)holderExpr).getHolder().getOutputSizeEstimator();
        final int argSize = holderExpr.args.size();
        ArrayList<OutputWidthExpression> arguments = null;
        if (argSize != 0) {
            arguments = new ArrayList<>(argSize);
            for (LogicalExpression expr : holderExpr.args) {
                arguments.add(expr.accept(this, state));
            }
        }
        return new  FunctionCallExpr(holderExpr, estimator, arguments);
    }

    @Override
    public OutputWidthExpression visitValueVectorWriteExpression(ValueVectorWriteExpression writeExpr,
                                                                 OutputWidthVisitorState state) throws RuntimeException {
        TypedFieldId fieldId = writeExpr.getFieldId();
        ProjectMemoryManager manager = state.getManager();
        OutputWidthExpression outputExpr = null;
        if (manager.isFixedWidth(fieldId)) {
            manager.addFixedWidthField(fieldId, state.getOutputColumnType());
            return null;
        } else {
            LogicalExpression writeArg = writeExpr.getChild();
            outputExpr = writeArg.accept(this, state);
        }
        return outputExpr;
    }

    @Override
    public OutputWidthExpression visitValueVectorReadExpression(ValueVectorReadExpression readExpr,
                                                                OutputWidthVisitorState state) throws RuntimeException {
        return new VarLenReadExpr(readExpr);
    }

    @Override
    public OutputWidthExpression visitQuotedStringConstant(ValueExpressions.QuotedString quotedString,
                                                           OutputWidthVisitorState state) throws RuntimeException {
        return new FixedLenExpr(quotedString.getString().length());
    }

    @Override
    public OutputWidthExpression visitUnknown(LogicalExpression logicalExpression, OutputWidthVisitorState state) {
        OutputWidthExpression fixedLenExpr = getFixedLenExpr(logicalExpression.getMajorType());
        if (fixedLenExpr != null) { return fixedLenExpr; }
        return null;
    }

    @Override
    public OutputWidthExpression visitFixedLenExpr(FixedLenExpr fixedLenExpr, OutputWidthVisitorState state)
            throws RuntimeException {
        return fixedLenExpr;
    }

    @Override
    public OutputWidthExpression visitVarLenReadExpr(VarLenReadExpr varLenReadExpr, OutputWidthVisitorState state)
                                                        throws RuntimeException {
        String columnName = varLenReadExpr.getName();
        if (columnName == null) {
            TypedFieldId fieldId = varLenReadExpr.getReadExpression().getTypedFieldId();
            ValueVector vv = state.manager.getIncomingValueVector(fieldId);
            columnName =  vv.getField().getName();
        }
        int columnWidth = state.manager.getColumnSize(columnName).getDataSizePerEntry();
        return new FixedLenExpr(columnWidth);
    }

    @Override
    public OutputWidthExpression visitFunctionCallExpr(FunctionCallExpr functionCallExpr, OutputWidthVisitorState state)
                                                        throws RuntimeException {
        ArrayList<OutputWidthExpression> args = functionCallExpr.getArgs();
        ArrayList<FixedLenExpr> estimatedArgs = new ArrayList<>(args.size());
        for (OutputWidthExpression expr : args) {
            FixedLenExpr fixedLenExpr = (FixedLenExpr) expr.accept(this, state);
            estimatedArgs.add(fixedLenExpr);
        }
        OutputSizeEstimator estimator = functionCallExpr.getEstimator();
        int estimatedSize = estimator.getEstimatedOutputSize(estimatedArgs);
        return new FixedLenExpr(estimatedSize);
    }


    private OutputWidthExpression getFixedLenExpr(MajorType majorType) {
        MajorType type = majorType;
        if (Types.isFixedWidthType(type)) {
            int fixedWidth = ProjectMemoryManager.getWidthOfFixedWidthType(type);
            return new OutputWidthExpression.FixedLenExpr(fixedWidth);
        }
        return null;
    }

}
