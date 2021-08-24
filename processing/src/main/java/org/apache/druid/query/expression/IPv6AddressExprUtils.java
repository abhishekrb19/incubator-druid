package org.apache.druid.query.expression;

import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv6.IPv6Address;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.regex.Pattern;

public class IPv6AddressExprUtils
{

  private static final Pattern IPV6_PATTERN = Pattern.compile(
      "(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"
  ); // this pattern is probably broken.

  private static final Pattern IPV6_BIGINT_PATTERN = Pattern.compile("\\d+");

  static boolean isIPv6(String address) {
    if (address == null) {
      return false;
    }
    IPAddressString addStr = new IPAddressString(address);
    return addStr.isIPv6() || (IPV6_BIGINT_PATTERN.matcher(address).matches() &&  new IPv6Address(new BigInteger(address)).isIPv6());
  }

  @Nullable
  static IPv6Address parse(@Nullable String string)
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

  // Maybe don't support this yet. Or two longs.
  static IPv6Address parse(BigInteger address) {
    return new IPv6Address(address);
  }

  static String toString(IPv6Address address)
  {
    return address.toString();
  }

  static BigInteger toBigInteger(IPv6Address address)
  {
    return address.getValue();
  }
}
