// The docs' sidebar, also the order of the pages in /llms.txt and /llms-full.txt.
export const sidebar = [
  {
    label: 'Start here',
    items: [
      { label: 'Getting started', slug: 'docs/getting-started' },
      { label: 'Change events', slug: 'docs/change-events' },
      { label: 'Permanent replication slots', slug: 'docs/replication-slots' },
      { label: 'Delivery and deduplication', slug: 'docs/deduplication' },
    ],
  },
  {
    label: 'Outputs',
    items: [
      { label: 'Kafka', slug: 'docs/outputs/kafka' },
      { label: 'RabbitMQ', slug: 'docs/outputs/rabbitmq' },
      { label: 'RabbitMQ Streams', slug: 'docs/outputs/rabbitmq-streams' },
      { label: 'Redis', slug: 'docs/outputs/redis' },
      { label: 'SQLite', slug: 'docs/outputs/sqlite' },
      { label: 'MongoDB', slug: 'docs/outputs/mongodb' },
      { label: 'Amazon Kinesis', slug: 'docs/outputs/kinesis' },
      { label: 'Amazon DynamoDB', slug: 'docs/outputs/dynamodb' },
      { label: 'Azure Event Hubs', slug: 'docs/outputs/event-hubs' },
      { label: 'Webhooks', slug: 'docs/outputs/webhooks' },
    ],
  },
  {
    label: 'Reference',
    items: [{ label: 'Configuration', slug: 'docs/configuration' }],
  },
];

export const docSlugs = sidebar.flatMap((group) => group.items.map((item) => item.slug));
