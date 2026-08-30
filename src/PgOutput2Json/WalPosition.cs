using System;

namespace PgOutput2Json
{
    /// <summary>
    /// Deduplication position of a published message: the transaction final LSN ("w")
    /// plus the message number within that transaction ("n", reset by every BeginMessage).
    /// Positions order lexicographically - the WAL position first, the message number second.
    /// </summary>
    /// <remarks>
    /// <see cref="Zero"/> is the "no position" value: data copy rows are published without
    /// any LSN info, while real WAL positions always carry a non-zero LSN. A zero position
    /// is never a duplicate, and an unknown or empty partition contributes <see cref="Zero"/>
    /// to the safe minimum, forcing a full replay instead of silent data loss.
    /// </remarks>
    internal readonly struct WalPosition
    {
        public static readonly WalPosition Zero = default;

        public WalPosition(ulong walSeq, ulong messageNo)
        {
            WalSeq = walSeq;
            MessageNo = messageNo;
        }

        public ulong WalSeq { get; }

        public ulong MessageNo { get; }

        public bool IsZero => WalSeq == 0 && MessageNo == 0;

        /// <summary>
        /// Returns true if this position is at or below the last published one.
        /// </summary>
        public bool IsAtOrBelow(in WalPosition last)
        {
            return WalSeq < last.WalSeq || (WalSeq == last.WalSeq && MessageNo <= last.MessageNo);
        }

        /// <summary>
        /// Returns true if this position is strictly above the last published one.
        /// </summary>
        public bool IsAfter(in WalPosition last)
        {
            return WalSeq > last.WalSeq || (WalSeq == last.WalSeq && MessageNo > last.MessageNo);
        }

        /// <summary>
        /// Returns true if this position was already published - it is at or below the
        /// last published position. The zero position is never a duplicate.
        /// </summary>
        public bool IsDuplicate(in WalPosition last)
        {
            return !IsZero && IsAtOrBelow(last);
        }

        /// <summary>
        /// Returns the lower of the two positions - the safe resume point, since everything
        /// at or below it is already durably published everywhere.
        /// </summary>
        public static WalPosition Min(in WalPosition left, in WalPosition right)
        {
            return left.IsAtOrBelow(right) ? left : right;
        }

        public override string ToString() => $"{WalSeq}/{MessageNo}";
    }
}