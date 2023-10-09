package com.RUStore;

public enum constants {
    oPut( 100, 4 ),
    oGet( 200, 4 ),
    oRemove( 300, 4 ),
    oList( 400, 4 ),
    oDisconnect( 500, 4 );

    public static final int byteSize = 4;

    public static final int existedKey = 1;
    public static final int notExistedKey = 1;
    public static final int success = 0;

    public static final String keySeparator = "|";
    private final int code;
    private final int byteLength;

    constants ( final int code,
                    final int byteLength )
    {
        this.code = code;
        this.byteLength = byteLength;
    }

    public int getCode ()
    {
        return code;
    }

    public int getByteLength ()
    {
        return byteLength;
    }
}
