import { Kafka, logLevel } from 'kafkajs';

let producerPromise = null;

function parseBrokers() {
  const raw = process.env.KAFKA_BROKERS ?? '';
  const list = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
  return list;
}

function buildKafkaConfig() {
  const brokers = parseBrokers();
  if (brokers.length === 0) {
    throw new Error('KAFKA_BROKERS is required (comma-separated host:port list)');
  }

  const clientId = process.env.KAFKA_CLIENT_ID || 'kafka-test-publisher';
  const username = process.env.KAFKA_USERNAME;
  const password = process.env.KAFKA_PASSWORD;

  const config = {
    clientId,
    brokers,
    logLevel: logLevel.NOTHING,
  };

  if (username != null && username !== '' && password != null && password !== '') {
    const mechanism = (process.env.KAFKA_SASL_MECHANISM || 'scram-sha-512').toLowerCase();
    const securityProtocol = (
      process.env.KAFKA_SECURITY_PROTOCOL || 'SASL_PLAINTEXT'
    ).toUpperCase();

    config.ssl =
      securityProtocol === 'SASL_SSL' || securityProtocol === 'SSL';
    config.sasl = {
      mechanism,
      username,
      password,
    };
  }

  return config;
}

/**
 * Lazily creates and connects a single Kafka producer for the process lifetime.
 */
export function getProducer() {
  if (!producerPromise) {
    producerPromise = (async () => {
      const kafka = new Kafka(buildKafkaConfig());
      const producer = kafka.producer({
        allowAutoTopicCreation: false,
      });
      await producer.connect();
      return producer;
    })().catch((e) => {
      producerPromise = null;
      throw e;
    });
  }
  return producerPromise;
}

export async function disconnectProducer() {
  if (!producerPromise) {
    return;
  }
  try {
    const producer = await producerPromise;
    await producer.disconnect();
  } catch {
    // ignore disconnect errors during shutdown
  } finally {
    producerPromise = null;
  }
}
