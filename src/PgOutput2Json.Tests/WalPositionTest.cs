using Microsoft.VisualStudio.TestTools.UnitTesting;

using PgOutput2Json;

namespace PgOutput2Json.Tests
{
    [TestClass]
    public class WalPositionTest
    {
        [TestMethod]
        [DataRow(0ul, 0ul, "0/0")]
        [DataRow(1ul, 2ul, "1/2")]
        [DataRow(18446744073709551615ul, 0ul, "18446744073709551615/0")]
        public void ToString_should_format_wal_seq_and_message_no(ulong walSeq, ulong messageNo, string expected)
        {
            Assert.AreEqual(expected, new WalPosition(walSeq, messageNo).ToString());
        }

        [TestMethod]
        public void Zero_should_be_the_default_position()
        {
            Assert.AreEqual(0ul, WalPosition.Zero.WalSeq);
            Assert.AreEqual(0ul, WalPosition.Zero.MessageNo);
            Assert.IsTrue(WalPosition.Zero.IsZero);
            Assert.IsTrue(default(WalPosition).IsZero); // default(WalPosition) is Zero
        }

        [TestMethod]
        [DataRow(0ul, 0ul, 0ul, 0ul, false, DisplayName = "zero position is never a duplicate")]
        [DataRow(0ul, 0ul, 5ul, 2ul, false, DisplayName = "zero position is never a duplicate of a real watermark")]
        [DataRow(5ul, 2ul, 5ul, 2ul, true, DisplayName = "equal position is a duplicate")]
        [DataRow(5ul, 1ul, 5ul, 2ul, true, DisplayName = "lower message no is a duplicate")]
        [DataRow(5ul, 3ul, 5ul, 2ul, false, DisplayName = "higher message no is not a duplicate")]
        [DataRow(4ul, 9ul, 5ul, 0ul, true, DisplayName = "lower wal seq is a duplicate")]
        [DataRow(6ul, 0ul, 5ul, 2ul, false, DisplayName = "higher wal seq is not a duplicate")]
        public void IsDuplicate_should_skip_only_real_positions_at_or_below_the_watermark(
            ulong walSeq, ulong messageNo, ulong lastWalSeq, ulong lastMessageNo, bool expected)
        {
            Assert.AreEqual(expected, new WalPosition(walSeq, messageNo).IsDuplicate(new WalPosition(lastWalSeq, lastMessageNo)));
        }

        [TestMethod]
        [DataRow(5ul, 2ul, 5ul, 2ul, false, DisplayName = "equal position is not after")]
        [DataRow(5ul, 3ul, 5ul, 2ul, true, DisplayName = "higher message no is after")]
        [DataRow(5ul, 1ul, 5ul, 2ul, false, DisplayName = "lower message no is not after")]
        [DataRow(6ul, 0ul, 5ul, 2ul, true, DisplayName = "higher wal seq is after")]
        [DataRow(4ul, 9ul, 5ul, 0ul, false, DisplayName = "lower wal seq is not after")]
        [DataRow(0ul, 0ul, 5ul, 2ul, false, DisplayName = "zero position is never after a real watermark")]
        public void IsAfter_should_move_forward_only(ulong walSeq, ulong messageNo, ulong lastWalSeq, ulong lastMessageNo, bool expected)
        {
            Assert.AreEqual(expected, new WalPosition(walSeq, messageNo).IsAfter(new WalPosition(lastWalSeq, lastMessageNo)));
        }

        [TestMethod]
        [DataRow(5ul, 2ul, 3ul, 9ul, 3ul, 9ul, DisplayName = "lower wal seq wins")]
        [DataRow(3ul, 9ul, 5ul, 2ul, 3ul, 9ul, DisplayName = "lower wal seq wins, mirrored")]
        [DataRow(5ul, 2ul, 5ul, 1ul, 5ul, 1ul, DisplayName = "same wal seq - lower message no wins")]
        [DataRow(5ul, 1ul, 5ul, 2ul, 5ul, 1ul, DisplayName = "same wal seq - lower message no wins, mirrored")]
        [DataRow(5ul, 2ul, 5ul, 2ul, 5ul, 2ul, DisplayName = "equal positions")]
        [DataRow(0ul, 0ul, 5ul, 2ul, 0ul, 0ul, DisplayName = "unknown partition (0,0) collapses the minimum to a full replay")]
        public void Min_should_return_the_safe_resume_point(ulong aWalSeq, ulong aMessageNo, ulong bWalSeq, ulong bMessageNo, ulong expectedWalSeq, ulong expectedMessageNo)
        {
            var min = WalPosition.Min(new WalPosition(aWalSeq, aMessageNo), new WalPosition(bWalSeq, bMessageNo));

            Assert.AreEqual(expectedWalSeq, min.WalSeq);
            Assert.AreEqual(expectedMessageNo, min.MessageNo);
        }
    }
}