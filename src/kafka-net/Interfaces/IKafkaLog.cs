namespace KafkaNet
{
    public interface IKafkaLog
    {
        /// <summary>
        /// Record debug information using the String.Format syntax.
        /// </summary>
        /// <param name="format">Format string template. e.g. "Exception = {0}"</param>
        /// <param name="args">Arguments which will fill the template string in order of apperance.</param>
        void DebugFormat(string format, params object[] args);
        /// <summary>
        /// Record info information using the String.Format syntax.
        /// </summary>
        /// <param name="format">Format string template. e.g. "Exception = {0}"</param>
        /// <param name="args">Arguments which will fill the template string in order of apperance.</param>
        void InfoFormat(string format, params object[] args);
        /// <summary>
        /// Record warning information using the String.Format syntax.
        /// </summary>
        /// <param name="format">Format string template. e.g. "Exception = {0}"</param>
        /// <param name="args">Arguments which will fill the template string in order of apperance.</param>
        void WarnFormat(string format, params object[] args);
        /// <summary>
        /// Record error information using the String.Format syntax.
        /// </summary>
        /// <param name="format">Format string template. e.g. "Exception = {0}"</param>
        /// <param name="args">Arguments which will fill the template string in order of apperance.</param>
        void ErrorFormat(string format, params object[] args);
        /// <summary>
        /// Record fatal information using the String.Format syntax.
        /// </summary>
        /// <param name="format">Format string template. e.g. "Exception = {0}"</param>
        /// <param name="args">Arguments which will fill the template string in order of apperance.</param>
        void FatalFormat(string format, params object[] args);
    }
}
