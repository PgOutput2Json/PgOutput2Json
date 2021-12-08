namespace PgOutput2Json.RabbitMq
{
    public static class PgOutput2JsonBuilderExtensions
    {
        public static PgOutput2JsonBuilder UseRabbitMq(this PgOutput2JsonBuilder builder, 
            Action<RabbitMqOptions>? configureAction = null)
        {
            var options = new RabbitMqOptions();

            configureAction?.Invoke(options);

            builder.WithMessagePublisherFactory(new RabbitMqPublisherFactory(options));

            return builder;
        }
    }
}
