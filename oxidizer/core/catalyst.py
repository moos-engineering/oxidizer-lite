import json

from .lattice import RedisConnection, KafkaConnection
from .residue import Residue



class RedisCatalyst:
    """Implement Catalyst stream operations with Redis Streams.

    Structured messages are JSON-encoded into the stream's ``message`` field.
    Reads use Redis consumer groups and leave acknowledgement to the caller so
    workflow code can commit only after its processing succeeds.
    """

    def __init__(self, connection: RedisConnection, residue: Residue):
        """Initialize the Redis Streams implementation behind Catalyst.

        Stores the connection used for stream operations, the logger used for
        operation events, and the backend identifier used in those events.

        Args:
            connection: Redis connection wrapper containing the Redis client.
            residue: Component-scoped logger supplied by the Catalyst facade.
        """
        self.connection = connection
        self.residue = residue
        self.backend = "redis"

    @staticmethod
    def is_json(value) -> bool:
        """Return whether a string or byte value contains valid JSON.

        Args:
            value: Candidate Redis field value.

        Returns:
            ``True`` for valid JSON text; otherwise ``False``.
        """
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            return False
        try:
            json.loads(value)
        except (TypeError, ValueError):
            return False
        return True

    def create_stream(self, topic_name: str):
        """Ensure a Redis Stream key exists without retaining a seed entry.

        Redis creates stream keys on the first ``XADD``. This method adds and
        immediately trims a temporary entry only when the key is absent.

        Args:
            topic_name: Redis key to create as a stream.
        """
        if not self.connection.client.exists(topic_name):
            self.connection.client.xadd(topic_name, {"initial": "entry"})
            self.connection.client.xtrim(topic_name, maxlen=0)  # Remove the initial entry immediately after creating the stream
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Created stream",
            backend=self.backend,
            topic_name=topic_name,
        )
        return

    def create_consumer_group(self, topic_name: str, group_name: str, start_id: str = "0"):
        """Create a consumer group, tolerating an existing group.

        ``mkstream=True`` creates the stream key if needed. The default start
        ID lets the group read existing entries; ``$`` can be supplied to begin
        with entries added after group creation.

        Args:
            topic_name: Stream key that owns the group.
            group_name: Consumer-group name.
            start_id: Redis stream ID from which the group begins reading.

        Raises:
            Any Redis error except the expected ``BUSYGROUP`` duplicate error.
        """
        try:
            self.connection.client.xgroup_create(topic_name, group_name, id=start_id, mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Created consumer group",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
        )
        return

    def write_to_stream(self, topic_name: str, message: str, *, emit_log: bool = True):
        """Append one value to a Redis Stream.

        Mappings and lists are JSON-encoded; scalar values are stringified.
        The payload is stored under the ``message`` field used by Catalyst's
        read path.

        Args:
            topic_name: Stream key to append to.
            message: Mapping, list, or scalar payload.
            emit_log: Whether to emit a Catalyst debug event for this write.

        Returns:
            Redis-generated stream entry ID.
        """
        data = {"message": json.dumps(message) if isinstance(message, (dict, list)) else str(message)} # Ensure all values are strings or JSON-encoded
        msg_id = self.connection.client.xadd(topic_name, data)
        if emit_log:
            self.residue.emit(
                self.residue.ash.DEBUG,
                "CATALYST: Wrote message",
                backend=self.backend,
                topic_name=topic_name,
                message_id=msg_id,
            )
        return msg_id

    def write_many_to_stream(self, topic_name: str, messages: list, *, emit_log: bool = True) -> list:
        """Append ordered entries in non-atomic pipelines of at most 1,000 records.

        Errors propagate; earlier entries or chunks may already have been
        written, so retry can duplicate output just as with single writes.
        """
        message_ids = []
        for offset in range(0, len(messages), 1000):
            with self.connection.client.pipeline(transaction=False) as pipeline:
                for message in messages[offset:offset + 1000]:
                    payload = json.dumps(message) if isinstance(message, (dict, list)) else str(message)
                    pipeline.xadd(topic_name, {"message": payload})
                message_ids.extend(pipeline.execute())
        if emit_log and messages:
            self.residue.emit(
                self.residue.ash.DEBUG,
                "CATALYST: Wrote message batch",
                backend=self.backend,
                topic_name=topic_name,
                message_count=len(messages),
            )
        return message_ids

    def read_from_stream(self, topic_name: str, group_name: str, consumer_name: str, count: int = 1, block: int = 1000, stream_id: str = ">"):
        """Read entries as a named consumer in a Redis consumer group.

        ``stream_id='>'`` requests new entries not yet delivered to the group;
        ``'0'`` reads pending entries owned by the consumer group. Byte keys
        and values are decoded, and JSON-valued fields are parsed.

        Args:
            topic_name: Redis stream key.
            group_name: Consumer group whose offset is advanced by this read.
            consumer_name: Consumer identity within the group.
            count: Maximum number of entries to return.
            block: Maximum wait in milliseconds; zero means block indefinitely.
            stream_id: Redis stream ID selector, usually ``>`` for new messages.

        Returns:
            List of ``(message_id, fields)`` pairs. Entries remain pending until
            the caller acknowledges them with :meth:`ack_message`.
        """
        messages = self.connection.client.xreadgroup(group_name, consumer_name, {topic_name: stream_id}, count=count, block=block) 
        messages = [(msg_id.decode('utf-8') if isinstance(msg_id, bytes) else msg_id, {k.decode('utf-8') if isinstance(k, bytes) else k: json.loads(v) if self.is_json(v) else v.decode('utf-8') if isinstance(v, bytes) else v for k, v in msg_data.items()}) for _, msgs in messages for msg_id, msg_data in msgs]
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Read messages",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_count=len(messages),
        )
        
        return messages

    def read_stream_range(self, topic_name: str, count: int = 10000):
        """Read recent Redis stream entries without creating a group or acking them.

        Entries are returned newest first and decoded using the same field rules
        as ``read_from_stream``. This inspection read emits no Catalyst log
        event, avoiding writes back into the stream being inspected.
        """
        if type(count) is not int or count < 1:
            raise ValueError("count must be a positive integer")
        messages = self.connection.client.xrevrange(topic_name, count=count)
        decoded = [
            (
                message_id.decode("utf-8") if isinstance(message_id, bytes) else str(message_id),
                {
                    key.decode("utf-8") if isinstance(key, bytes) else str(key):
                    json.loads(value) if self.is_json(value) else value.decode("utf-8") if isinstance(value, bytes) else value
                    for key, value in fields.items()
                },
            )
            for message_id, fields in messages
        ]
        return decoded

    def list_streams(self, pattern: str | None = None) -> list[str]:
        """List Redis stream keys, using client capabilities when available.

        Args:
            pattern: Optional Redis glob pattern; defaults to all keys.

        Returns:
            Sorted unique stream names. Clients without scanning support return
            whatever stream keys their fallback exposes.
        """
        client = self.connection.client
        streams: list[str] = []
        if hasattr(client, "scan_iter"):
            try:
                for key in client.scan_iter(match=pattern or "*", _type="stream"):
                    streams.append(key.decode("utf-8") if isinstance(key, bytes) else str(key))
            except (TypeError, Exception):
                try:
                    for key in client.scan_iter(match=pattern or "*"):
                        streams.append(key.decode("utf-8") if isinstance(key, bytes) else str(key))
                except Exception:
                    pass
        elif hasattr(client, "keys"):
            try:
                for key in client.keys(pattern or "*"):
                    streams.append(key.decode("utf-8") if isinstance(key, bytes) else str(key))
            except Exception:
                pass
        elif hasattr(client, "streams") and isinstance(client.streams, dict):
            streams = [str(k) for k in client.streams.keys()]

        if pattern and streams and pattern != "*":
            import fnmatch
            streams = [s for s in streams if fnmatch.fnmatch(s, pattern)]
        return sorted(set(streams))

    def get_transport_health(self, topic_name: str, group_name: str | None = None) -> dict:
        """Inspect one Redis stream without reading or acknowledging entries."""
        client = self.connection.client
        stream_type = client.type(topic_name)
        if isinstance(stream_type, bytes):
            stream_type = stream_type.decode("utf-8")
        if stream_type == "none":
            raise LookupError("Stream not found")
        if stream_type != "stream":
            raise ValueError("The requested Redis key is not a stream")
        groups = []
        for raw_group in client.xinfo_groups(topic_name):
            group = {
                key.decode("utf-8") if isinstance(key, bytes) else key:
                value.decode("utf-8") if isinstance(value, bytes) else value
                for key, value in raw_group.items()
            }
            if group_name is None or group["name"] == group_name:
                groups.append({
                    "name": group["name"],
                    "consumers": group.get("consumers"),
                    "pending": group.get("pending"),
                    "last_delivered_id": group.get("last-delivered-id"),
                    "lag": group.get("lag"),
                })
        if group_name is not None and not groups:
            raise LookupError("Consumer group not found")
        return {
            "backend": "redis", "stream": topic_name,
            "length": client.xlen(topic_name), "groups": groups[:100],
            "groups_truncated": len(groups) > 100,
        }

    def ack_message(self, topic_name: str, group_name: str, message_id: str, consumer_name: str | None = None):
        """Acknowledge a pending entry for a Redis consumer group.

        Args:
            topic_name: Stream containing the entry.
            group_name: Consumer group tracking the entry.
            message_id: Redis stream entry ID to acknowledge.
            consumer_name: Optional identity used for diagnostic logging.
        """
        self.connection.client.xack(topic_name, group_name, message_id)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Acknowledged message",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_id=message_id,
        )
        return

    def ack_messages(
        self,
        topic_name: str,
        group_name: str,
        message_ids: list[str],
        consumer_name: str | None = None,
    ) -> int:
        """Acknowledge multiple Redis entries with one XACK command."""
        if not message_ids:
            return 0
        acknowledged = self.connection.client.xack(topic_name, group_name, *message_ids)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Acknowledged messages",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_count=len(message_ids),
            acknowledged_count=acknowledged,
        )
        return acknowledged

    def delete_message(self, topic_name: str, message_id: str, group_name: str | None = None, consumer_name: str | None = None):
        """Delete a stream entry, independently of consumer acknowledgement.

        Args:
            topic_name: Stream containing the entry.
            message_id: Redis stream entry ID to delete.
            group_name: Optional diagnostic-only consumer-group name.
            consumer_name: Optional diagnostic-only consumer name.
        """
        self.connection.client.xdel(topic_name, message_id)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Deleted message",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_id=message_id,
        )
        return

    def set_ttl(self, topic_name: str, message_id: str, ttl: int):
        """Set expiration on the whole stream key, not one entry.

        Args:
            topic_name: Redis stream key to expire.
            message_id: Retained for API symmetry and logging; Redis expiration
                applies to the stream key rather than this individual ID.
            ttl: Expiration duration in seconds.
        """
        self.connection.client.expire(topic_name, ttl)
        self.residue.emit(
            self.residue.ash.WARNING,
            "CATALYST: Redis stream TTL applies to the stream key, not an individual message",
            backend=self.backend,
            topic_name=topic_name,
            message_id=message_id,
            ttl=ttl,
        )
        return



class KafkaCatalyst:
    """Implement the Catalyst stream interface with Confluent Kafka clients.

    Kafka topics replace Redis stream keys. Consumer configuration and group
    identity are owned by the injected ``KafkaConnection``; some Catalyst
    parameters remain for interface parity but are not Kafka-native controls.
    """

    def __init__(self, connection: KafkaConnection, residue: Residue):
        """Bind configured Kafka clients and a component-scoped logger.

        Args:
            connection: Kafka wrapper holding admin, producer, and consumer
                clients as configured by the caller.
            residue: Logger used for Catalyst operation events.
        """
        self.connection = connection
        self.residue = residue
        self.backend = "kafka"

    @staticmethod
    def _topic_name(topic_name: str) -> str:
        return topic_name.replace(":", ".")

    def create_stream(self, topic_name: str):
        """Create a one-partition, one-replica topic if it does not exist.

        The existing-topic error is ignored; other Kafka errors propagate.

        Args:
            topic_name: Kafka topic to create.
        """
        from confluent_kafka import KafkaError, KafkaException
        from confluent_kafka.admin import NewTopic

        topic_name = self._topic_name(topic_name)
        futures = self.connection.admin_client.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
        try:
            futures[topic_name].result()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if not isinstance(error, KafkaError) or error.code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Created stream",
            backend=self.backend,
            topic_name=topic_name,
        )
        return

    def create_consumer_group(self, topic_name: str, group_name: str, start_id: str = "0"):
        """Ensure the topic exists before its configured consumer subscribes.

        Kafka's group ID is configured on the connection's consumer; ``group_name``
        and ``start_id`` remain interface-compatible diagnostic values.

        Args:
            topic_name: Topic associated with the requested group.
            group_name: Group name for diagnostic logging.
            start_id: Redis-compatible argument unused by this adapter.
        """
        self.create_stream(topic_name)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Prepared Kafka consumer group",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
        )
        return

    def write_to_stream(self, topic_name: str, message: str, *, emit_log: bool = True):
        """Produce one record and synchronously flush the producer.

        Mappings and lists are JSON-encoded; scalar values are stringified.

        Args:
            topic_name: Kafka topic to produce to.
            message: Mapping, list, or scalar payload.
            emit_log: Whether to emit a Catalyst debug event for this write.

        Returns:
            ``None``; Kafka's producer API does not return a stream entry ID.
        """
        topic_name = self._topic_name(topic_name)
        value = json.dumps(message) if isinstance(message, (dict, list)) else str(message)
        self.connection.producer.produce(topic_name, value=value)
        self.connection.producer.flush()
        if emit_log:
            self.residue.emit(
                self.residue.ash.DEBUG,
                "CATALYST: Wrote message",
                backend=self.backend,
                topic_name=topic_name,
            )
        return

    def write_many_to_stream(self, topic_name: str, messages: list, *, emit_log: bool = True) -> list:
        """Publish bounded batches and return one placeholder per Kafka record."""
        if not messages:
            return []
        topic_name = self._topic_name(topic_name)
        producer = self.connection.producer
        for start in range(0, len(messages), 5000):
            for message in messages[start:start + 5000]:
                value = json.dumps(message) if isinstance(message, (dict, list)) else str(message)
                while True:
                    try:
                        producer.produce(topic_name, value=value)
                        producer.poll(0)
                        break
                    except BufferError:
                        producer.poll(0.1)
            if producer.flush(timeout=60):
                raise TimeoutError("Kafka producer still has undelivered records")
        if emit_log:
            self.residue.emit(
                self.residue.ash.DEBUG,
                "CATALYST: Wrote message batch",
                backend=self.backend,
                topic_name=topic_name,
                message_count=len(messages),
            )
        return [None] * len(messages)

    def read_from_stream(self, topic_name: str, group_name: str, consumer_name: str, count: int, block: int, stream_id: str = ">"):
        """Subscribe to a topic and consume up to ``count`` records.

        The actual Kafka consumer group and identity come from the injected
        consumer configuration. ``group_name``, ``consumer_name``, and
        ``stream_id`` are retained for the shared Catalyst signature and log
        context; Kafka offsets are managed by the configured consumer.

        Args:
            topic_name: Kafka topic to subscribe to.
            group_name: Shared-interface group label used in diagnostics.
            consumer_name: Shared-interface consumer label used in diagnostics.
            count: Maximum number of records to consume.
            block: Maximum consume wait in milliseconds.
            stream_id: Shared-interface offset selector; unused by Kafka.

        Returns:
            List of ``(partition:offset, decoded_value)`` pairs.

        Raises:
            RuntimeError: If a consumed Kafka record reports an error.
        """
        topic_name = self._topic_name(topic_name)
        self.connection.consumer.subscribe([topic_name])
        records = self.connection.consumer.consume(num_messages=count, timeout=block / 1000)
        messages = []
        for record in records:
            if record.error():
                raise RuntimeError(record.error())
            value = record.value()
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            messages.append((f"{record.partition()}:{record.offset()}", json.loads(value) if RedisCatalyst.is_json(value) else value))
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Read messages",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_count=len(messages),
        )
        return messages

    def list_streams(self, pattern: str | None = None) -> list[str]:
        """Return no topics; topic discovery is not implemented for Kafka."""
        return []

    def get_transport_health(self, topic_name: str, group_name: str | None = None) -> dict:
        """Describe a topic and offsets through admin APIs, without consuming."""
        from confluent_kafka import ConsumerGroupTopicPartitions, TopicCollection, TopicPartition
        from confluent_kafka.admin import OffsetSpec

        admin = self.connection.admin_client
        if admin is None:
            raise RuntimeError("Kafka admin client is required for transport inspection")
        kafka_topic = self._topic_name(topic_name)
        description = admin.describe_topics(
            TopicCollection([kafka_topic]), request_timeout=10,
        )[kafka_topic].result(timeout=10)
        partitions = [TopicPartition(kafka_topic, partition.id) for partition in description.partitions]
        if len(partitions) > 100:
            raise ValueError("Transport inspection supports at most 100 Kafka partitions")
        earliest = admin.list_offsets(
            {partition: OffsetSpec.earliest() for partition in partitions}, request_timeout=10,
        )
        latest = admin.list_offsets(
            {partition: OffsetSpec.latest() for partition in partitions}, request_timeout=10,
        )
        committed_offsets = {}
        if group_name is not None:
            group = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_name, partitions)], request_timeout=10,
            )[group_name].result(timeout=10)
            for partition in group.topic_partitions:
                if partition.error:
                    raise RuntimeError("Kafka committed offset lookup failed")
                if partition.offset >= 0:
                    committed_offsets[partition.partition] = partition.offset
        offsets = []
        for partition in partitions:
            start = earliest[partition].result(timeout=10).offset
            end = latest[partition].result(timeout=10).offset
            committed = committed_offsets.get(partition.partition)
            out_of_range = committed is not None and not start <= committed <= end
            offsets.append({
                "partition": partition.partition,
                "start_offset": start,
                "end_offset": end,
                "committed_offset": committed,
                "lag": end - committed if committed is not None and not out_of_range else None,
                "offset_out_of_range": out_of_range,
            })
        return {
            "backend": "kafka", "stream": topic_name, "topic": kafka_topic,
            "group": group_name, "partitions": offsets,
        }

    def ack_message(self, topic_name: str, group_name: str, message_id: str, consumer_name: str | None = None):
        """Commit the specific partition offset returned by the reader."""
        self.ack_messages(topic_name, group_name, [message_id], consumer_name)
        return

    def ack_messages(
        self,
        topic_name: str,
        group_name: str,
        message_ids: list[str],
        consumer_name: str | None = None,
    ) -> int:
        """Commit the supplied partition offsets for a batch of messages."""
        if not message_ids:
            return 0
        from confluent_kafka import TopicPartition

        next_offsets: dict[int, int] = {}
        for message_id in message_ids:
            partition, offset = (int(value) for value in message_id.split(":"))
            next_offsets[partition] = max(next_offsets.get(partition, 0), offset + 1)
        kafka_topic = self._topic_name(topic_name)
        offsets = [
            TopicPartition(kafka_topic, partition, offset)
            for partition, offset in next_offsets.items()
        ]
        self.connection.consumer.commit(offsets=offsets, asynchronous=False)
        self.residue.emit(
            self.residue.ash.DEBUG,
            "CATALYST: Committed Kafka message offsets",
            backend=self.backend,
            topic_name=topic_name,
            group_name=group_name,
            consumer_name=consumer_name,
            message_count=len(message_ids),
        )
        return len(message_ids)

    def delete_message(self, topic_name: str, message_id: str, group_name: str | None = None, consumer_name: str | None = None):
        """Log that Kafka does not support deleting an individual record by ID.

        Args:
            topic_name: Topic containing the record.
            message_id: Partition/offset label; deletion is not performed.
            group_name: Unused shared-interface argument.
            consumer_name: Unused shared-interface argument.
        """
        self.residue.emit(
            self.residue.ash.WARNING,
            "CATALYST: Kafka does not support deleting individual messages by message ID",
            backend=self.backend,
            topic_name=topic_name,
            message_id=message_id,
        )
        return

    def set_ttl(self, topic_name: str, message_id: str, ttl: int):
        """Warn that Kafka retention is topic-level, not per-message.

        Args:
            topic_name: Topic whose retention policy would need configuration.
            message_id: Unused per-record identifier.
            ttl: Requested duration; no topic configuration is changed.
        """
        self.residue.emit(
            self.residue.ash.WARNING,
            "CATALYST: Kafka retention is configured at the topic level, not per message",
            backend=self.backend,
            topic_name=topic_name,
            message_id=message_id,
            ttl=ttl,
        )
        return



class Catalyst:
    """Backend-neutral facade for Redis Streams and Kafka operations.

    The facade selects an adapter from the connection type and exposes one
    interface. Broker differences remain observable for message IDs, consumer
    groups, stream discovery, deletion, and retention.
    """

    def __init__(self, connection: RedisConnection | KafkaConnection, residue: Residue | None = None):
        """Select the backend adapter and bind its logger.

        Args:
            connection: Redis or Kafka connection wrapper.
            residue: Optional shared logger; a Catalyst-scoped child is bound
                when omitted.

        Raises:
            ValueError: If the connection type is unsupported.
        """
        self.connection = connection
        self.residue = (residue or Residue()).bind(component_name="catalyst")
        if isinstance(connection, RedisConnection):
            self.catalyst = RedisCatalyst(connection, self.residue)
        elif isinstance(connection, KafkaConnection):
            self.catalyst = KafkaCatalyst(connection, self.residue)
        else:
            raise ValueError("Unsupported connection type.")

    def create_stream(self, topic_name: str):
        """
        Ensure the Redis stream or Kafka topic exists. Redis creates an empty
        stream key; Kafka uses the adapter's configured topic defaults.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic to create.
        """
        self.catalyst.create_stream(topic_name)
        return

    def create_consumer_group(self, topic_name: str, group_name: str, start_id: str = "0"):
        """
        Prepare consumer-group use for a stream or topic. Redis creates the
        group (and stream if needed); Kafka creates groups on commit and uses
        the group ID configured on its consumer.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            group_name (str): The name of the consumer group to create.
        """
        self.catalyst.create_consumer_group(topic_name, group_name, start_id=start_id)
        return

    def write_to_stream(self, topic_name: str, message: str, *, emit_log: bool = True):
        """
        Write one payload to a Redis stream or Kafka topic. Mappings and lists
        are JSON-encoded; scalar values are stringified by the backend.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            message (str): Payload to encode and publish.
            emit_log (bool): Whether to emit a Catalyst debug event.
        
        Returns:
            Redis stream entry ID, or ``None`` for Kafka, whose producer does
            not return an entry ID through this interface.
        """
        msg_id = self.catalyst.write_to_stream(topic_name, message, emit_log=emit_log)
        return msg_id

    def write_many_to_stream(self, topic_name: str, messages: list, *, emit_log: bool = True) -> list:
        """Publish ordered records, using bounded pipelines for Redis.

        Backends without bulk publishing retain single-write behavior.
        """
        write_many = getattr(self.catalyst, "write_many_to_stream", None)
        if callable(write_many):
            return write_many(topic_name, messages, emit_log=emit_log)
        return [self.write_to_stream(topic_name, message, emit_log=emit_log) for message in messages]

    def read_from_stream(self, topic_name: str, group_name: str, consumer_name: str, count=1, block=1000, stream_id=">"):
        """
        Read a batch using the configured backend consumer. Redis uses the
        group, consumer, and stream ID to control delivery; Kafka uses the
        consumer's configured group and subscription.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            group_name (str): The name of the consumer group.
            start_id (str): Redis starting stream ID; unused by Kafka.
            consumer_name (str): The name of the consumer within the group.
            count (int): The maximum number of messages to read at once.
            block (int): The time in milliseconds to block if no messages are available.
            stream_id (str): Stream entry id offset to read from (defaults to '>' for new messages, '0' for pending).
        
        Returns:
            list: ``(message_id, payload)`` pairs. Redis IDs are stream IDs;
            Kafka IDs are formatted as ``partition:offset``.
        """
        messages = self.catalyst.read_from_stream(topic_name, group_name, consumer_name, count=count, block=block, stream_id=stream_id)

        if messages:
            self.residue.emit(self.residue.ash.DEBUG, f"CATALYST: Read from stream '{topic_name}'")
        else:
            self.residue.emit(self.residue.ash.DEBUG, f"CATALYST: No new messages in stream '{topic_name}' for consumer '{consumer_name}'.")
        return messages

    def read_stream_range(self, topic_name: str, count: int = 10000):
        """Read recent Redis stream entries without consumer-group side effects.

        Historical range reads are not supported by the Kafka adapter.
        """
        if not isinstance(self.connection, RedisConnection):
            raise NotImplementedError("Historical stream range reads are only supported by Redis")
        return self.catalyst.read_stream_range(topic_name, count=count)

    def list_streams(self, pattern: str | None = None) -> list[str]:
        """
        List discoverable streams matching an optional pattern. Redis scans
        stream keys; the Kafka adapter currently returns an empty list because
        topic discovery is not implemented there.
        """
        if hasattr(self.catalyst, "list_streams"):
            return self.catalyst.list_streams(pattern=pattern)
        return []

    def get_transport_health(self, topic_name: str, group_name: str | None = None) -> dict:
        """Return read-only metadata for one stream/topic on this connection."""
        if not isinstance(topic_name, str) or not topic_name.strip():
            raise ValueError("stream_name must be a non-empty string")
        if group_name is not None and (not isinstance(group_name, str) or not group_name.strip()):
            raise ValueError("group_name must be a non-empty string when supplied")
        return self.catalyst.get_transport_health(topic_name, group_name)

    def ack_message(self, topic_name: str, group_name: str, msg_id: str, consumer_name: str | None = None):
        """
        Mark a consumed message as processed for its backend. Redis acknowledges
        one entry ID within a group; Kafka commits the configured consumer's
        current offsets synchronously.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            group_name (str): The name of the consumer group within the Redis stream / Kafka topic.
            msg_id (str): The ID of the stream entry to acknowledge.
            consumer_name (str | None): Optional diagnostic consumer label.
        """    
        self.catalyst.ack_message(topic_name, group_name, msg_id, consumer_name=consumer_name)
        return

    def ack_messages(
        self,
        topic_name: str,
        group_name: str,
        message_ids: list[str],
        consumer_name: str | None = None,
    ) -> int:
        """Acknowledge a batch through the selected backend adapter."""
        return self.catalyst.ack_messages(
            topic_name, group_name, message_ids, consumer_name=consumer_name
        )

    def delete_message(self, topic_name: str, msg_id: str, group_name: str | None = None, consumer_name: str | None = None):
        """
        Delete a Redis stream entry. Kafka cannot delete an individual record
        by ID and emits a warning instead.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            msg_id (str): The ID of the stream entry to delete.
            group_name (str | None): Optional diagnostic consumer-group name.
            consumer_name (str | None): Optional diagnostic consumer name.
        """
        self.catalyst.delete_message(topic_name, msg_id, group_name=group_name, consumer_name=consumer_name)
        return

    def set_ttl(self, topic_name: str, msg_id: str, ttl: int):
        """
        Sets a time-to-live (TTL) for backends that support it.

        Redis applies TTL to the stream key. Kafka does not support per-message TTL.
        
        Args:
            topic_name (str): The name of the Redis stream / Kafka topic.
            msg_id (str): The stream entry ID retained for API context and logging.
            ttl (int): The TTL in seconds.
        """
        self.catalyst.set_ttl(topic_name, msg_id, ttl)
        return

    