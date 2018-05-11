package org.apache.drill.exec.expr;

import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FunctionCallExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.FixedLenExpr;
import org.apache.drill.exec.physical.impl.project.OutputWidthExpression.VarLenReadExpr;


public abstract class AbstractExecExprVisitor<T, VAL, EXCEP extends Exception> extends AbstractExprVisitor<T, VAL, EXCEP> {

    public T visitValueVectorWriteExpression(ValueVectorWriteExpression writeExpr, VAL value) throws EXCEP {
        return visitUnknown(writeExpr, value);
    }

    public T visitValueVectorReadExpression(ValueVectorReadExpression readExpr, VAL value) throws EXCEP {
        return visitUnknown(readExpr, value);
    }

    public T visitFunctionCallExpr(FunctionCallExpr functionCallExpr, VAL value) throws EXCEP {
        return visitUnknown(functionCallExpr, value);
    }

    public T visitFixedLenExpr(FixedLenExpr fixedLenExpr, VAL value) throws EXCEP {
        return visitUnknown(fixedLenExpr, value);
    }

    public T visitVarLenReadExpr(VarLenReadExpr varLenReadExpr, VAL value) throws EXCEP {
        return visitUnknown(varLenReadExpr, value);
    }

    public T visitUnknown(OutputWidthExpression e, VAL value) throws EXCEP {
        throw new UnsupportedOperationException(String.format("Expression of type %s not handled by visitor type %s.", e.getClass().getCanonicalName(), this.getClass().getCanonicalName()));
    }

}
