/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.expression;

import inet.ipaddr.ipv6.IPv6Address;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExprType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.util.List;

/**
 * <pre>
 * Implements an expression that parses a string or long into an IPv6 address stored (as an unsigned
 * int) in a long.
 *
 * Expression signatures:
 * - long ipv6_parse(string)
 * - long ipv6_parse(long)
 *
 * String arguments should be formatted as a dotted-decimal.
 * Long arguments that can be represented as an IPv6 address are passed through.
 * Invalid arguments return null.
 * </pre>
 *
 * @see IPv4AddressStringifyExprMacro
 * @see IPv4AddressMatchExprMacro
 */
public class IPv6AddressParseExprMacro implements ExprMacroTable.ExprMacro
{
  public static final String FN_NAME = "ipv6_parse";

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

    class IPv6AddressParseExpr extends ExprMacroTable.BaseScalarUnivariateMacroFunctionExpr
    {
      private IPv6AddressParseExpr(Expr arg)
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
            return ExprEval.ofLongArray(null);
        }
      }

      @Override
      public Expr visit(Shuttle shuttle)
      {
        Expr newArg = arg.visit(shuttle);
        return shuttle.visit(new IPv6AddressParseExpr(newArg));
      }

      @Nullable
      @Override
      public ExprType getOutputType(InputBindingInspector inspector)
      {
        return ExprType.STRING;
      }
    }

    return new IPv6AddressParseExpr(arg);
  }

  private static ExprEval evalAsString(ExprEval eval)
  {
    if (eval.asString() == null) {
      return eval;
    }
    IPv6Address iPv6Address = IPv6AddressExprUtils.parseToIPv6Address(eval.asString());
    return ExprEval.of(IPv6AddressExprUtils.v6AddressToNumericString(iPv6Address));
  }
}
