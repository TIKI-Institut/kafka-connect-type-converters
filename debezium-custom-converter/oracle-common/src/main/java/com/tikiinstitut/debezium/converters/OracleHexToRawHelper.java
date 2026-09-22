package com.tikiinstitut.debezium.converters;

import oracle.sql.NUMBER;

import java.math.BigDecimal;

/**
 * Decodes the numeric values Oracle delivers as text: the snapshot reads through JDBC and yields
 * BigDecimal, but LogMiner parses the redo statement and yields String, sometimes wrapped in
 * HEXTORAW('..').
 *
 * The HEXTORAW handling is extracted source code from the official Debezium project at
 * [io.debezium.connector.oracle.OracleValueConverters]
 * to avoid a direct dependency.
 * As otherwise binary incompatible changes in these methods would break our SPI implementation.
 */
public final class OracleHexToRawHelper {

    /*
     * Copyright Debezium Authors.
     *
     * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
     */
    public static final String HEXTORAW_FUNCTION_START = "HEXTORAW('";
    public static final String HEXTORAW_FUNCTION_END = "')";

    private OracleHexToRawHelper() {
    }

    public static boolean isHexToRawFunctionCall(String value) {
        return value != null && value.startsWith(HEXTORAW_FUNCTION_START) && value.endsWith(HEXTORAW_FUNCTION_END);
    }

    public static String getHexToRawHexString(String hexToRawValue) {
        if (isHexToRawFunctionCall(hexToRawValue)) {
            return hexToRawValue.substring(HEXTORAW_FUNCTION_START.length(),
                    hexToRawValue.length() - HEXTORAW_FUNCTION_END.length());
        }
        return hexToRawValue;
    }

    public static byte[] convertHexToRawFunctionToByteArray(String value) {
        final String rawValue = getHexToRawHexString(value);
        int len = rawValue.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(rawValue.charAt(i), 16) << 4)
                    + Character.digit(rawValue.charAt(i + 1), 16));
        }
        return data;
    }

    // Throws NumberFormatException when the text is not a number, callers decide how to handle it
    public static BigDecimal toBigDecimal(String value) {
        if (isHexToRawFunctionCall(value)) {
            return new BigDecimal(new NUMBER(convertHexToRawFunctionToByteArray(value)).stringValue());
        }
        return new BigDecimal(value);
    }
}
