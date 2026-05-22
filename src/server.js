import 'dotenv/config';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getProducer, disconnectProducer } from './producer.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const publicDir = path.join(__dirname, '..', 'public');

const app = express();
app.use(express.json({ limit: '10mb' }));

function kafkaHeadersFromObject(obj) {
  if (obj == null || typeof obj !== 'object' || Array.isArray(obj)) {
    return {};
  }
  /** @type {Record<string, Buffer>} */
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    if (typeof k !== 'string' || k === '') continue;
    if (typeof v !== 'string') {
      throw new Error(`Header "${k}" must be a string value`);
    }
    out[k] = Buffer.from(v, 'utf8');
  }
  return out;
}

/**
 * Kafdrop / tooling export: { key, value: { payload, encoding }, headers: [{ key, value: { payload, encoding } }] }
 */
function isEnvelopeRecord(item) {
  return (
    item != null &&
    typeof item === 'object' &&
    !Array.isArray(item) &&
    item.value != null &&
    typeof item.value === 'object' &&
    Object.prototype.hasOwnProperty.call(item.value, 'payload') &&
    (Object.prototype.hasOwnProperty.call(item.value, 'encoding') ||
      Array.isArray(item.headers) ||
      item.key !== undefined)
  );
}

/** If value.payload is a nested full message (bad export), return inner { headers, key, value }. */
function tryUnwrapNestedEnvelope(valueObj) {
  if (valueObj == null || typeof valueObj !== 'object') return null;
  const { payload, encoding } = valueObj;
  if (encoding !== 'json' || payload == null || typeof payload !== 'object' || Array.isArray(payload)) {
    return null;
  }
  if (
    Array.isArray(payload.headers) &&
    payload.value != null &&
    typeof payload.value === 'object' &&
    Object.prototype.hasOwnProperty.call(payload.value, 'payload')
  ) {
    return {
      headers: payload.headers,
      key: payload.key,
      value: payload.value,
    };
  }
  return null;
}

function headerWireValueToBuffer(hv) {
  if (hv == null) {
    return Buffer.alloc(0);
  }
  if (typeof hv === 'string') {
    return Buffer.from(hv, 'utf8');
  }
  if (typeof hv !== 'object') {
    return Buffer.from(String(hv), 'utf8');
  }
  const { payload, encoding } = hv;
  if (encoding === 'none') {
    if (payload == null || (typeof payload === 'object' && Object.keys(payload).length === 0)) {
      return Buffer.alloc(0);
    }
  }
  if (payload == null) {
    return Buffer.alloc(0);
  }
  if (encoding === 'json' && typeof payload !== 'string') {
    return Buffer.from(JSON.stringify(payload), 'utf8');
  }
  return Buffer.from(typeof payload === 'string' ? payload : String(payload), 'utf8');
}

/** @param {unknown} headersArr */
function envelopeHeadersToMap(headersArr) {
  /** @type {Record<string, Buffer>} */
  const out = {};
  if (!Array.isArray(headersArr)) {
    return out;
  }
  for (const h of headersArr) {
    if (h == null || typeof h !== 'object' || typeof h.key !== 'string' || h.key === '') {
      continue;
    }
    out[h.key] = headerWireValueToBuffer(h.value);
  }
  return out;
}

/**
 * @param {unknown} keyObj
 * @param {Buffer | undefined} fallback
 */
function envelopeKeyToBuffer(keyObj, fallback) {
  if (keyObj == null) {
    return fallback;
  }
  if (typeof keyObj === 'string') {
    return Buffer.from(keyObj, 'utf8');
  }
  if (typeof keyObj !== 'object') {
    return Buffer.from(String(keyObj), 'utf8');
  }
  const enc = keyObj.encoding;
  const payload = keyObj.payload;
  if (enc === 'none' || payload == null) {
    return fallback;
  }
  if (typeof payload === 'object' && Object.keys(payload).length === 0) {
    return fallback;
  }
  if (typeof payload === 'string') {
    return Buffer.from(payload, 'utf8');
  }
  return Buffer.from(String(payload), 'utf8');
}

function envelopeValueToBuffer(valueObj) {
  if (valueObj == null || typeof valueObj !== 'object') {
    throw new Error('envelope value must be an object');
  }
  const { payload, encoding } = valueObj;
  if (encoding === 'json' && typeof payload !== 'string') {
    const json = JSON.stringify(payload);
    if (json === undefined) {
      throw new Error('value.payload serializes to undefined');
    }
    return Buffer.from(json, 'utf8');
  }
  if (typeof payload === 'string') {
    return Buffer.from(payload, 'utf8');
  }
  if (payload == null) {
    throw new Error('value.payload is required');
  }
  return Buffer.from(String(payload), 'utf8');
}

function mergeHeaderBuffers(base, extra) {
  return { ...base, ...extra };
}

/**
 * @param {unknown} item
 * @param {number} index
 * @param {{ headerMap: Record<string, Buffer>, keyBuffer: Buffer | undefined }} defaults
 */
function recordToKafkaParts(item, index, defaults) {
  if (!isEnvelopeRecord(item)) {
    try {
      const json = JSON.stringify(item);
      if (json === undefined) {
        throw new Error(`messages[${index}] serializes to undefined`);
      }
      const hm = { ...defaults.headerMap };
      return {
        key: defaults.keyBuffer,
        value: Buffer.from(json, 'utf8'),
        headers: Object.keys(hm).length ? hm : undefined,
      };
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`messages[${index}]: ${msg}`);
    }
  }

  const record = /** @type {{ headers?: unknown, key?: unknown, value: object }} */ (item);
  const inner = tryUnwrapNestedEnvelope(record.value);

  let headerMap = mergeHeaderBuffers(defaults.headerMap, envelopeHeadersToMap(record.headers));
  if (inner) {
    headerMap = mergeHeaderBuffers(headerMap, envelopeHeadersToMap(inner.headers));
  }

  let keyBuf = defaults.keyBuffer;
  keyBuf = envelopeKeyToBuffer(record.key, keyBuf);
  if (inner) {
    keyBuf = envelopeKeyToBuffer(inner.key, keyBuf);
  }

  const valueObj = inner ? inner.value : record.value;
  const value = envelopeValueToBuffer(valueObj);

  const headerKeys = Object.keys(headerMap);
  return {
    key: keyBuf,
    value,
    headers: headerKeys.length ? headerMap : undefined,
  };
}

app.post('/api/publish', async (req, res) => {
  try {
    const { topic, headers, messages, key } = req.body ?? {};

    if (typeof topic !== 'string' || topic.trim() === '') {
      res.status(400).json({ ok: false, error: 'topic is required (non-empty string)' });
      return;
    }

    if (!Array.isArray(messages)) {
      res.status(400).json({ ok: false, error: 'messages must be a JSON array' });
      return;
    }

    if (messages.length === 0) {
      res.status(400).json({ ok: false, error: 'messages must contain at least one element' });
      return;
    }

    let defaultHeaderMap;
    try {
      defaultHeaderMap = kafkaHeadersFromObject(headers);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      res.status(400).json({ ok: false, error: msg });
      return;
    }

    let defaultKeyBuffer;
    if (key !== undefined && key !== null) {
      if (typeof key !== 'string') {
        res.status(400).json({ ok: false, error: 'key must be a string when provided' });
        return;
      }
      defaultKeyBuffer = Buffer.from(key, 'utf8');
    }

    const kafkaMessages = [];
    const errors = [];

    for (let i = 0; i < messages.length; i++) {
      try {
        const parts = recordToKafkaParts(messages[i], i, {
          headerMap: defaultHeaderMap,
          keyBuffer: defaultKeyBuffer,
        });
        kafkaMessages.push({
          key: parts.key,
          value: parts.value,
          headers: parts.headers,
        });
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        errors.push({ index: i, error: msg });
      }
    }

    if (errors.length > 0) {
      res.status(400).json({
        ok: false,
        error: 'One or more messages could not be serialized',
        errors,
      });
      return;
    }

    const producer = await getProducer();
    await producer.send({
      topic: topic.trim(),
      messages: kafkaMessages,
    });

    res.json({
      ok: true,
      published: kafkaMessages.length,
    });
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    const isBrokerConfig = msg.includes('KAFKA_BROKERS');
    res.status(isBrokerConfig ? 503 : 500).json({ ok: false, error: msg });
  }
});

app.use(express.static(publicDir));

const port = Number(process.env.PORT) || 3010;
const server = app.listen(port, () => {
  console.log(`kafka-publisher listening on http://localhost:${port}`);
});

async function shutdown(signal) {
  console.log(`${signal}: shutting down…`);
  await new Promise((resolve) => server.close(resolve));
  await disconnectProducer();
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
