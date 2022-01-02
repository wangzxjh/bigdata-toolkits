//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.huawei.bigdata.cdm;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Base64Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Base64Utils.class);
  private static final int RANGE = 255;
  private static final char[] BASE64_BYTE_TO_STR = new char[]{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'L', 'K', 'J', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
  private static byte[] strToBase64Byte = new byte[128];

  public Base64Utils() {
  }

  public static String encodeUtf8(String source) throws UnsupportedEncodingException {
    return encode(source.getBytes("UTF-8"));
  }

  public static String decodeUtf8(String val) throws UnsupportedEncodingException {
    return new String(decode(val), "UTF-8");
  }

  public static String encode(byte[] bytes) {
    StringBuilder res = new StringBuilder();

    for(int i = 0; i <= bytes.length - 1; i += 3) {
      byte[] enBytes = new byte[4];
      byte tmp = 0;

      int k;
      for(k = 0; k <= 2; ++k) {
        if (i + k <= bytes.length - 1) {
          enBytes[k] = (byte)((bytes[i + k] & 255) >>> 2 + 2 * k | tmp);
          tmp = (byte)(((bytes[i + k] & 255) << 2 + 2 * (2 - k) & 255) >>> 2);
        } else {
          enBytes[k] = tmp;
          tmp = 64;
        }
      }

      enBytes[3] = tmp;

      for(k = 0; k <= 3; ++k) {
        if (enBytes[k] <= 63) {
          res.append(BASE64_BYTE_TO_STR[enBytes[k]]);
        } else {
          res.append('=');
        }
      }
    }

    return res.toString();
  }

  private static boolean isBase64(String str) {
    String base64Pattern = "^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$";
    return Pattern.matches(base64Pattern, str);
  }

  public static byte[] decode(String val) throws UnsupportedEncodingException {
    boolean isEncode = isBase64(val);
    if (!isEncode) {
      throw new UnsupportedEncodingException();
    } else {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] srcBytes = val.getBytes("utf-8");
      byte[] base64bytes = new byte[srcBytes.length];

      int i;
      for(i = 0; i <= srcBytes.length - 1; ++i) {
        int ind = srcBytes[i];
        base64bytes[i] = strToBase64Byte[ind];
      }

      for(i = 0; i <= base64bytes.length - 1; i += 4) {
        byte[] deBytes = new byte[3];
        int delen = 0;

        int k;
        for(k = 0; k <= 2; ++k) {
          if (i + k + 1 <= base64bytes.length - 1 && base64bytes[i + k + 1] >= 0) {
            byte tmp = (byte)((base64bytes[i + k + 1] & 255) >>> 2 + 2 * (2 - (k + 1)));
            deBytes[k] = (byte)((base64bytes[i + k] & 255) << 2 + 2 * k & 255 | tmp);
            ++delen;
          }
        }

        for(k = 0; k <= delen - 1; ++k) {
          bos.write(deBytes[k]);
        }
      }

      return bos.toByteArray();
    }
  }

  public static void main(String[] args) throws UnsupportedEncodingException {
    if (args.length < 2) {
      LOG.info("as least 2 parameter required");
      System.exit(-1);
    }

    String mode = args[0];
    String params = String.join(" ", (CharSequence[])Arrays.copyOfRange(args, 1, args.length));
    String result = "";
    if (mode.equals("1")) {
      result = encodeUtf8(params);
    } else if (mode.equals("2")) {
      result = decodeUtf8(params);
    } else {
      LOG.info("invalid mode, please input 1 or 2");
      System.exit(-1);
    }

    System.out.println(result);
  }

  private static boolean writeFile(String filePath, String jsonString) {
    try {
      FileWriter fw = new FileWriter(filePath);
      PrintWriter out = new PrintWriter(fw);
      out.write(jsonString);
      out.println();
      fw.close();
      out.close();
      return true;
    } catch (IOException var4) {
      var4.printStackTrace();
      return false;
    }
  }

  static {
    int i;
    for(i = 0; i <= strToBase64Byte.length - 1; ++i) {
      strToBase64Byte[i] = -1;
    }

    for(i = 0; i <= BASE64_BYTE_TO_STR.length - 1; ++i) {
      strToBase64Byte[BASE64_BYTE_TO_STR[i]] = (byte)i;
    }

  }
}
