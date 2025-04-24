namespace PgOutput2Json
{
    // taken from: https://github.com/postgres/postgres/blob/90260e2ec6bbfc3dfa9d9501ab75c535de52f677/src/include/catalog/pg_type.dat

    public enum PgOid
    {
        BOOLOID = 16,
        BYTEAOID = 17,
        CHAROID = 18,
        NAMEOID = 19,
        INT8OID = 20,
        INT2OID = 21,
        INT2VECTOROID = 22,
        INT4OID = 23,
        REGPROCOID = 24,
        TEXTOID = 25,
        OIDOID = 26,
        TIDOID = 27,
        XIDOID = 28,
        CIDOID = 29,
        OIDVECTOROID = 30,

        POINTOID = 600,
        LSEGOID = 601,
        PATHOID = 602,
        BOXOID = 603,
        POLYGONOID = 604,
        LINEOID = 628,
        FLOAT4OID = 700,
        FLOAT8OID = 701,
        ABSTIMEOID = 702,
        RELTIMEOID = 703,
        TINTERVALOID = 704,
        UNKNOWNOID = 705,
        CIRCLEOID = 718,
        CASHOID = 790,
        INETOID = 869,
        CIDROID = 650,
        BPCHAROID = 1042,
        VARCHAROID = 1043,
        DATEOID = 1082,
        TIMEOID = 1083,
        TIMESTAMPOID = 1114,
        TIMESTAMPTZOID = 1184,
        INTERVALOID = 1186,
        TIMETZOID = 1266,
        ZPBITOID = 1560,
        VARBITOID = 1562,
        NUMERICOID = 1700,

        A_BOOLOID = 1000,
        A_BYTEAOID = 1001,
        A_INT2OID = 1005,
        A_INT4OID = 1007,
        A_INT8OID = 1016,
        A_OIDOID = 1028,
        A_FLOAT4OID = 1021,
        A_FLOAT8OID = 1022,
        A_NUMERICOID = 1231,

        A_BPCHAROID = 1014,
        A_VARCHAROID = 1015,

        JSONOID = 114,
        A_JSONOID = 199, // array of json

        XMLOID = 142,
        A_XMLOID = 143, // array of xml
    }

    internal static class PgOidExtensions
    {
        public static bool IsNumber(this PgOid pgOid)
        {
            return pgOid == PgOid.INT2OID
                || pgOid == PgOid.INT4OID
                || pgOid == PgOid.INT8OID
                || pgOid == PgOid.OIDOID
                || pgOid == PgOid.FLOAT4OID
                || pgOid == PgOid.FLOAT8OID
                || pgOid == PgOid.NUMERICOID;
        }

        public static bool IsBoolean(this PgOid pgOid)
        {
            return pgOid == PgOid.BOOLOID;
        }

        public static bool IsByte(this PgOid pgOid)
        {
            return pgOid == PgOid.BYTEAOID;
        }

        public static bool IsArrayOfNumber(this PgOid pgOid)
        {
            return pgOid == PgOid.A_INT2OID
                || pgOid == PgOid.A_INT4OID
                || pgOid == PgOid.A_INT8OID
                || pgOid == PgOid.A_OIDOID
                || pgOid == PgOid.A_FLOAT4OID
                || pgOid == PgOid.A_FLOAT8OID
                || pgOid == PgOid.A_NUMERICOID;
        }

        public static bool IsArrayOfText(this PgOid pgOid)
        {
            return pgOid == PgOid.A_BPCHAROID
                || pgOid == PgOid.A_VARCHAROID
                || pgOid == PgOid.A_JSONOID
                || pgOid == PgOid.A_XMLOID;
        }

        public static bool IsArrayOfBoolean(this PgOid pgOid)
        {
            return pgOid == PgOid.A_BOOLOID;
        }

        public static bool IsArrayOfByte(this PgOid pgOid)
        {
            return pgOid == PgOid.A_BYTEAOID;
        }

        /*
        public static bool IsArray(this PgOid pgOid)
        {
            return IsArrayOfBoolean(pgOid)
                || IsArrayOfByte(pgOid)
                || IsArrayOfNumber(pgOid)
                || IsArrayOfString(pgOid);
        }
        */
    }
}
