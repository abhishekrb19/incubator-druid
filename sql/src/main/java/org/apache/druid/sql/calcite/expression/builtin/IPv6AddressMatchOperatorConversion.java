package org.apache.druid.sql.calcite.expression.builtin;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.expression.IPv6AddressMatchExprMacro;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.OperatorConversions;

public class IPv6AddressMatchOperatorConversion extends DirectOperatorConversion
{
  private static final SqlSingleOperandTypeChecker ADDRESS_OPERAND = OperandTypes.or(
      OperandTypes.family(SqlTypeFamily.STRING)
  );

  private static final SqlSingleOperandTypeChecker SUBNET_OPERAND = OperandTypes.family(SqlTypeFamily.STRING);

  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(IPv6AddressMatchExprMacro.FN_NAME))
      .operandTypeChecker(OperandTypes.sequence("(expr,string)", ADDRESS_OPERAND, SUBNET_OPERAND))
      .returnTypeInference(ReturnTypes.BOOLEAN_NULLABLE)
      .functionCategory(SqlFunctionCategory.USER_DEFINED_FUNCTION)
      .build();

  public IPv6AddressMatchOperatorConversion()
  {
    super(SQL_FUNCTION, IPv6AddressMatchExprMacro.FN_NAME);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }
}
