package org.apache.druid.query.expression;

import inet.ipaddr.ipv6.IPv6Address;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public class IPv6AddressStringifyExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv6_stringify";

  @Override
  public String name()
  {
    return FN_NAME;
  }

  @Override
  public Expr apply(final List<Expr> args)
  {
    if (args.size() != 1) {
      throw new IAE(ExprUtils.createErrMsg(name(), "must have 1 argument"));
    }
    Expr arg = args.get(0);

    class IPv6AddressStringifyExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv6AddressStringifyExpr(Expr arg)
      {
        super(FN_NAME, arg);
      }

      @Nonnull
      @Override
      public ExprEval eval(final ObjectBinding bindings)
      {
        ExprEval eval = arg.eval(bindings);
        switch (eval.type()) {
          case STRING:
            return evalAsString(eval);
          default:
            return ExprEval.of(null);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new IPv6AddressStringifyExpr(newArg));
      }

      @Nullable
      @Override
      public ExprType getOutputType(InputBindingInspector inspector)
      {
        return ExprType.STRING;
      }
    }

    return new IPv6AddressStringifyExpr(arg);
  }


  private static ExprEval evalAsString(ExprEval eval)
  {
    if (eval.asString() == null) {
        return ExprEval.of(null);
    }

    IPv6Address parsedIpv6 = IPv6AddressExprUtils.parseToIPv6Address(eval.asString());
    return ExprEval.of(IPv6AddressExprUtils.v6AddressToCidrString(parsedIpv6));
  }
}
