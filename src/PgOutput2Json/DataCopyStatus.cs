namespace PgOutput2Json
{
    public class DataCopyStatus
    {
        public bool IsCompleted { get; set; }

        /// <summary>
        /// This is optionally populated by the client app, not used by the publisher.
        /// It allows the client do decide where to continue the copy process, based on the lastJson exported
        /// </summary>
        public string? AdditionalRowFilter { get; set; }

        /// <summary>
        /// This is optionally populated by the client app, not used by the publisher.
        /// It is used in compbination with AdditionalRowFilter, and allows the client do decide where to continue the copy process, 
        /// based on the lastJson exported
        /// </summary>
        public string? OrderByColumns { get; set; }
    }
}
