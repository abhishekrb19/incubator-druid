package org.apache.druid.query.expression;

import inet.ipaddr.IPAddressString;
import inet.ipaddr.IPAddressStringParameters;
import inet.ipaddr.ipv6.IPv6Address;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.regex.Pattern;


public class IPv6AddressExprUtils
{

  private static final Pattern IPV6_BIGINT_PATTERN = Pattern.compile("\\d+");

  static boolean isIPv6(String address) {
    if (address == null) {
      return false;
    }
    IPAddressStringParameters strParams = new IPAddressStringParameters.Builder().allow_inet_aton(false).allowPrefix(false).
        toParams();
    IPAddressString addStr = new IPAddressString(address);
    return addStr.isIPv6() || (IPV6_BIGINT_PATTERN.matcher(address).matches() &&  new IPv6Address(new BigInteger(address)).isIPv6());
  }

  @Nullable
  static IPv6Address parseToIPv6Address(@Nullable String string)
  {
    if (string == null) {
      return null;
    }

    IPAddressString addStr = new IPAddressString(string);
    if (addStr.isIPv6()) {
      return addStr.getAddress().toIPv6();
    }

    if (IPV6_BIGINT_PATTERN.matcher(string).matches()) {
      IPv6Address isIpv6Add = new IPv6Address(new BigInteger(string));
      if (isIpv6Add.isIPv6()) {
        return new IPv6Address(new BigInteger(string)).toIPv6();
      }
    }
    return null;
  }

  /**
   * @return IPv6 address dotted-decimal notated string
   */

  @Nullable
  static String v6AddressToCidrString(IPv6Address address)
  {
    if (address == null) {
      return null;
    }
    return address.toString();
  }

  @Nullable
  static String v6AddressToNumericString(IPv6Address address)
  {
    if (address == null) {
      return null;
    }
    return address.getValue().toString();
  }
}
