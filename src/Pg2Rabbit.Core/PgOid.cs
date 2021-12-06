namespace Pg2Rabbit.Core
{
    internal enum PgOid
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
        NUMERICOID = 1700
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
    }
}
