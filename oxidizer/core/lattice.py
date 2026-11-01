from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlsplit, urlunsplit

from .residue import Residue


def _masked_url(url: str | None) -> str | None:
    """Hide URL passwords before connection details are written to logs.

    URLs without a password, and empty values, are returned unchanged.
    Username, host, port, path, query, and fragment are preserved when a
    password is masked.
    """
    if not url:
        return None
    parsed = urlsplit(url)
    if parsed.password is None:
        return url
    username = parsed.username or ""
    hostname = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{username}:***@{hostname}{port}" if username else f"***@{hostname}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))

class Lattice(ABC):
    """Marker base for typed external-service connection wrappers.

    Subclasses expose the client or clients consumed by stream, storage, and
    table adapters. The base class does not define connection lifecycle or
    client behavior.
    """


class S3Connection(Lattice):
    """
    Configure an S3-compatible client and expose its bucket and key prefix.

    Credentials can come from the boto3 credential chain, a named profile, or
    explicit access keys. A supplied session is reused; a supplied client
    bypasses session and client construction. When ``role_arn`` is configured,
    the session assumes that role through STS before creating the S3 client.
    Custom endpoints and S3 addressing styles support compatible object stores.

    Args:
        bucket: Default bucket used by consumers of this connection.
        prefix: Optional key prefix; surrounding slashes are removed.
        region_name: AWS region used for the boto3 session and client.
        endpoint_url: Optional custom S3 endpoint.
        access_key_id: Explicit access key ID; required with
            ``secret_access_key`` when either explicit credential is supplied.
        secret_access_key: Explicit secret access key; required with
            ``access_key_id`` when either explicit credential is supplied.
        session_token: Optional token for temporary explicit credentials.
        aws_profile_name: Named AWS profile passed to boto3.
        role_arn: Optional role assumed with STS after session creation.
        role_session_name: STS session name used for the assume-role call.
        external_id: Optional external ID sent to STS.
        use_ssl: Whether to use TLS for the S3 client.
        verify: TLS certificate verification setting or CA bundle path.
        addressing_style: Optional botocore S3 addressing style, such as
            ``path`` for local S3-compatible services.
        signature_version: Signature version passed to botocore.
        session: Optional preconfigured boto3 session.
        client: Optional preconfigured S3 client. When provided, no session or
            client is created by this wrapper.
        lazy_client: Defer SDK initialization until ``client`` is accessed.
            Useful when DuckDB only needs the connection settings.
        residue: Optional shared logger; defaults to a lattice-scoped logger.

    Raises:
        ValueError: If explicit credentials are partially supplied.
        Exception: Client/session creation errors are logged and re-raised.

    Examples:

        S3Connection(bucket="my-bucket")

        S3Connection(
            bucket="my-bucket",
            endpoint_url="http://localhost:9000",
            access_key_id="rustfsadmin",
            secret_access_key="rustfsadmin",
            addressing_style="path",
        )

        S3Connection(
            bucket="my-bucket",
            aws_profile_name="dev-sso",
            role_arn="arn:aws:iam::123456789012:role/OxidizerRole",
        )
    """

    service_name = "s3"

    def __init__(
        self,
        *,
        bucket: str | None = None,
        prefix: str = "",
        region_name: str = "us-east-1",
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        aws_profile_name: str | None = None,
        role_arn: str | None = None,
        role_session_name: str = "OxidizerS3Session",
        external_id: str | None = None,
        use_ssl: bool = True,
        verify: bool | str = True,
        addressing_style: str | None = None,
        signature_version: str = "s3v4",
        session: Any = None,
        client: Any = None,
        lazy_client: bool = False,
        residue: Residue | None = None,
    ):
        """Store options, create missing SDK objects, and log the outcome."""
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.aws_profile_name = aws_profile_name # SSO Profile Name
        self.role_arn = role_arn
        self.role_session_name = role_session_name
        self.external_id = external_id
        self.use_ssl = use_ssl
        self.verify = verify
        self.addressing_style = addressing_style
        self.signature_version = signature_version
        self.residue = (residue or Residue()).bind(component_name="lattice")

        self.session = session
        self.client = client
        try:
            if self._client is None and not lazy_client:
                self.session = self.session or self._create_session()
                self.client = self._create_client()
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "LATTICE: Failed to create S3 connection",
                backend="s3",
                bucket=self.bucket,
                prefix=self.prefix,
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
                aws_profile_name=self.aws_profile_name,
                role_arn_set=self.role_arn is not None,
                injected_client=client is not None,
                injected_session=session is not None,
                error=str(exc),
            )
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "LATTICE: Created S3 connection",
            backend="s3",
            bucket=self.bucket,
            prefix=self.prefix,
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            aws_profile_name=self.aws_profile_name,
            role_arn_set=self.role_arn is not None,
            injected_client=client is not None,
            injected_session=session is not None,
        )

    @property
    def client(self):
        """Initialize the SDK on first use when lazy construction was requested."""
        if self._client is None:
            self.session = self.session or self._create_session()
            self._client = self._create_client()
        return self._client

    @client.setter
    def client(self, value):
        self._client = value

    def _create_session(self):
        """Create a boto3 session and optionally exchange credentials via STS.

        The configured profile and region seed the session. If any explicit
        credential field is set, both access key ID and secret key are
        required. When ``role_arn`` is set, STS credentials initialize the
        returned session.
        """
        import boto3

        session_kwargs: dict[str, Any] = {"region_name": self.region_name}

        if self.aws_profile_name:
            session_kwargs["profile_name"] = self.aws_profile_name

        if self.access_key_id or self.secret_access_key or self.session_token:
            if not self.access_key_id or not self.secret_access_key:
                raise ValueError("access_key_id and secret_access_key must be provided together")
            session_kwargs.update(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                aws_session_token=self.session_token,
            )

        session = boto3.Session(**session_kwargs)
        if not self.role_arn:
            return session

        assume_role_kwargs = {
            "RoleArn": self.role_arn,
            "RoleSessionName": self.role_session_name,
        }
        if self.external_id:
            assume_role_kwargs["ExternalId"] = self.external_id

        credentials = session.client("sts").assume_role(**assume_role_kwargs)["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.region_name,
        )

    def _create_client(self):
        """Create the S3 client with endpoint, TLS, signing, and address style."""
        from botocore.config import Config

        config_kwargs: dict[str, Any] = {
            "signature_version": self.signature_version,
        }
        if self.addressing_style:
            config_kwargs["s3"] = {"addressing_style": self.addressing_style}

        return self.session.client(
            self.service_name,
            endpoint_url=self.endpoint_url,
            use_ssl=self.use_ssl,
            verify=self.verify,
            config=Config(**config_kwargs),
        )

    def object_key(self, key: str) -> str:
        """Normalize a relative key and prepend this connection's prefix.

        Leading slashes are removed from ``key``. If no prefix is configured,
        the normalized key is returned unchanged.

        Args:
            key: Object key relative to the connection prefix.

        Returns:
            The normalized key, optionally prefixed.
        """
        key = key.lstrip("/")
        return f"{self.prefix}/{key}" if self.prefix else key


class IcebergRestConnection(Lattice):
    """Pair an S3 storage connection with an Iceberg REST catalog.

    This wrapper carries catalog metadata alongside the S3 connection used for
    warehouse storage. The S3 bucket, when set, must match ``warehouse`` and
    the storage connection must have an explicit endpoint. If no catalog
    endpoint is supplied, ``/iceberg`` is appended to that S3 endpoint.

    Args:
        storage_connection: S3 connection used for warehouse storage.
        warehouse: Required warehouse identifier, matching the S3 bucket when
            the latter is configured.
        catalog_endpoint: Optional explicit Iceberg REST catalog URL.
        signing_service: AWS signing service name passed to catalog consumers.

    Raises:
        TypeError: If ``storage_connection`` is not an ``S3Connection``.
        ValueError: If the warehouse is empty, conflicts with the S3 bucket, or
            the S3 endpoint is missing.
    """

    def __init__(
        self,
        storage_connection: S3Connection,
        *,
        warehouse: str,
        catalog_endpoint: str | None = None,
        signing_service: str = "s3",
    ):
        """Validate and retain the S3 warehouse and catalog settings."""
        if not isinstance(storage_connection, S3Connection):
            raise TypeError("IcebergRestConnection requires an S3Connection")
        if not warehouse:
            raise ValueError("warehouse must not be empty")
        if storage_connection.bucket and storage_connection.bucket != warehouse:
            raise ValueError("S3 bucket and Iceberg warehouse must match")
        if not storage_connection.endpoint_url:
            raise ValueError("Iceberg REST over S3 requires an explicit S3 endpoint_url")
        self.storage_connection = storage_connection
        self.warehouse = warehouse
        self.catalog_endpoint = catalog_endpoint or f"{storage_connection.endpoint_url.rstrip('/')}/iceberg"
        self.signing_service = signing_service


class S3TablesConnection(Lattice):
    """
    Configure an AWS S3 Tables client for table operations.

    Authentication follows boto3's credential chain, a named profile, or
    explicit access keys, with optional STS role assumption. A supplied session
    is reused, while a supplied client bypasses SDK object creation. Unlike
    :class:`S3Connection`, this wrapper has no bucket or object-key prefix.

    Args:
        region_name: AWS region used for the boto3 session and client.
        endpoint_url: Optional custom S3 Tables endpoint.
        access_key_id: Explicit access key ID; required with
            ``secret_access_key`` when either explicit credential is supplied.
        secret_access_key: Explicit secret access key; required with
            ``access_key_id`` when either explicit credential is supplied.
        session_token: Optional token for temporary explicit credentials.
        aws_profile_name: Named AWS profile passed to boto3.
        role_arn: Optional role assumed with STS after session creation.
        role_session_name: STS session name used for the assume-role call.
        external_id: Optional external ID sent to STS.
        use_ssl: Whether to use TLS for the service client.
        verify: TLS certificate verification setting or CA bundle path.
        signature_version: Signature version passed to botocore.
        session: Optional preconfigured boto3 session.
        client: Optional preconfigured S3 Tables client.
        residue: Optional shared logger; defaults to a lattice-scoped logger.

    Raises:
        ValueError: If explicit credentials are partially supplied.
        Exception: Client/session creation errors are logged and re-raised.

    Examples:

        S3TablesConnection(
            endpoint_url="http://localhost:9000",
            access_key_id="rustfsadmin",
            secret_access_key="rustfsadmin",
        )

        S3TablesConnection(
            aws_profile_name="dev-sso",
            role_arn="arn:aws:iam::123456789012:role/OxidizerRole",
        )
    """

    service_name = "s3tables"

    def __init__(
        self,
        *,
        region_name: str = "us-east-1",
        endpoint_url: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
        session_token: str | None = None,
        aws_profile_name: str | None = None,
        role_arn: str | None = None,
        role_session_name: str = "OxidizerS3TablesSession",
        external_id: str | None = None,
        use_ssl: bool = True,
        verify: bool | str = True,
        signature_version: str = "s3v4",
        session: Any = None,
        client: Any = None,
        residue: Residue | None = None,
    ):
        """Store options, create missing SDK objects, and log the outcome."""
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.aws_profile_name = aws_profile_name
        self.role_arn = role_arn
        self.role_session_name = role_session_name
        self.external_id = external_id
        self.use_ssl = use_ssl
        self.verify = verify
        self.signature_version = signature_version
        self.residue = (residue or Residue()).bind(component_name="lattice")

        self.session = session
        self.client = client
        try:
            if self.client is None:
                self.session = self.session or self._create_session()
                self.client = self._create_client()
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "LATTICE: Failed to create S3 Tables connection",
                backend="s3tables",
                region_name=self.region_name,
                endpoint_url=self.endpoint_url,
                aws_profile_name=self.aws_profile_name,
                role_arn_set=self.role_arn is not None,
                injected_client=client is not None,
                injected_session=session is not None,
                error=str(exc),
            )
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "LATTICE: Created S3 Tables connection",
            backend="s3tables",
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            aws_profile_name=self.aws_profile_name,
            role_arn_set=self.role_arn is not None,
            injected_client=client is not None,
            injected_session=session is not None,
        )

    def _create_session(self):
        """Create a boto3 session and optionally assume the configured role."""
        import boto3

        session_kwargs: dict[str, Any] = {"region_name": self.region_name}

        if self.aws_profile_name:
            session_kwargs["profile_name"] = self.aws_profile_name

        if self.access_key_id or self.secret_access_key or self.session_token:
            if not self.access_key_id or not self.secret_access_key:
                raise ValueError("access_key_id and secret_access_key must be provided together")
            session_kwargs.update(
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                aws_session_token=self.session_token,
            )

        session = boto3.Session(**session_kwargs)
        if not self.role_arn:
            return session

        assume_role_kwargs = {
            "RoleArn": self.role_arn,
            "RoleSessionName": self.role_session_name,
        }
        if self.external_id:
            assume_role_kwargs["ExternalId"] = self.external_id

        credentials = session.client("sts").assume_role(**assume_role_kwargs)["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=self.region_name,
        )

    def _create_client(self):
        """Create the S3 Tables client with endpoint, TLS, and signing options."""
        from botocore.config import Config

        return self.session.client(
            self.service_name,
            endpoint_url=self.endpoint_url,
            use_ssl=self.use_ssl,
            verify=self.verify,
            config=Config(signature_version=self.signature_version),
        )


class RedisConnection(Lattice):
    """
    Configure a Redis-compatible client for stream and storage use.

    Connection modes include a supplied redis-py client, connection pool,
    Sentinel master, Unix socket, cluster, URL, or host/port. When the wrapper
    creates a client, those modes are checked in that order. ``provider`` is a
    validated label; connection construction uses the installed ``redis``
    Python package.

    Args:
        provider: Provider label: ``redis``, ``valkey``, or ``elasticache``.
        url: Optional standalone or cluster connection URL.
        host: Hostname for host-based connections.
        port: Port for host-based connections.
        db: Database number for standalone or Unix-socket connections.
        username: Optional ACL username.
        password: Optional password or provider authentication token.
        credential_provider: Optional redis-py credential provider.
        ssl: Whether to enable TLS for host-based client construction.
        ssl_cert_reqs: TLS certificate verification policy.
        decode_responses: Whether redis-py decodes response bytes to strings.
        socket_timeout: Optional timeout for socket operations.
        socket_connect_timeout: Optional connection-establishment timeout.
        health_check_interval: redis-py connection health-check interval.
        cluster: Whether to construct a Redis Cluster client.
        sentinel_hosts: Sentinel host/port pairs; requires a service name.
        sentinel_service_name: Sentinel master service name.
        sentinel_kwargs: Options passed to redis-py's Sentinel constructor.
        unix_socket_path: Optional Unix-domain socket path.
        connection_pool: Existing pool, which takes precedence over other
            connection modes.
        protocol: Optional Redis protocol version.
        client_name: Optional connection client name.
        client: Optional preconfigured client; when supplied, skips client
            construction.
        residue: Optional shared logger; defaults to a lattice-scoped logger.
        **client_kwargs: Additional redis-py client options.

    Raises:
        ValueError: If ``provider`` is unsupported or Sentinel hosts are given
            without a service name.
        Exception: Client creation errors are logged and re-raised.

    Examples:

        RedisConnection(host="localhost", port=6379)

        RedisConnection(url="redis://localhost:6379/0")

        RedisConnection(
            host="localhost",
            port=6379,
            username="default",
            password="redis-password",
        )

        RedisConnection(
            url="rediss://default:redis-password@redis.example.com:6379/0",
            ssl=True,
        )

        RedisConnection(
            provider="valkey",
            url="valkey://localhost:6379/0",
        )

        RedisConnection(
            provider="elasticache",
            host="cache-cluster.xxxxxx.use1.cache.amazonaws.com",
            port=6379,
            ssl=True,
            cluster=True,
        )

        RedisConnection(
            provider="elasticache",
            host="cache.xxxxxx.use1.cache.amazonaws.com",
            port=6379,
            username="app-user",
            password="elasticache-auth-token",
            ssl=True,
        )

        RedisConnection(
            provider="elasticache",
            host="cache.xxxxxx.use1.cache.amazonaws.com",
            port=6379,
            username="iam-user",
            credential_provider=elasticache_iam_provider,
            ssl=True,
        )

        RedisConnection(
            sentinel_hosts=[("sentinel-1", 26379), ("sentinel-2", 26379)],
            sentinel_service_name="mymaster",
            password="redis-password",
        )
    """

    supported_providers = {"redis", "valkey", "elasticache"}

    def __init__(
        self,
        *,
        provider: str = "redis",
        url: str | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        credential_provider: Any = None,
        ssl: bool = False,
        ssl_cert_reqs: str | None = "required",
        decode_responses: bool = True,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        health_check_interval: int = 0,
        cluster: bool = False,
        sentinel_hosts: list[tuple[str, int]] | None = None,
        sentinel_service_name: str | None = None,
        sentinel_kwargs: dict[str, Any] | None = None,
        unix_socket_path: str | None = None,
        connection_pool: Any = None,
        protocol: int | None = None,
        client_name: str | None = None,
        client: Any = None,
        residue: Residue | None = None,
        **client_kwargs: Any,
    ):
        """Validate the provider, retain options, and create or inject a client."""
        if provider not in self.supported_providers:
            raise ValueError(f"provider must be one of: {', '.join(sorted(self.supported_providers))}")

        self.provider = provider
        self.url = url
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.credential_provider = credential_provider
        self.ssl = ssl
        self.ssl_cert_reqs = ssl_cert_reqs
        self.decode_responses = decode_responses
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.health_check_interval = health_check_interval
        self.cluster = cluster
        self.sentinel_hosts = sentinel_hosts
        self.sentinel_service_name = sentinel_service_name
        self.sentinel_kwargs = sentinel_kwargs or {}
        self.unix_socket_path = unix_socket_path
        self.connection_pool = connection_pool
        self.protocol = protocol
        self.client_name = client_name
        self.client_kwargs = client_kwargs
        self.residue = (residue or Residue()).bind(component_name="lattice")

        try:
            self.client = client if client is not None else self._create_client()
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "LATTICE: Failed to create Redis connection",
                backend="redis",
                provider=self.provider,
                url=_masked_url(self.url),
                host=self.host,
                port=self.port,
                db=self.db,
                ssl=self.ssl,
                cluster=self.cluster,
                sentinel=self.sentinel_hosts is not None,
                unix_socket_path_set=self.unix_socket_path is not None,
                injected_client=client is not None,
                error=str(exc),
            )
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "LATTICE: Created Redis connection",
            backend="redis",
            provider=self.provider,
            url=_masked_url(self.url),
            host=self.host,
            port=self.port,
            db=self.db,
            ssl=self.ssl,
            cluster=self.cluster,
            sentinel=self.sentinel_hosts is not None,
            unix_socket_path_set=self.unix_socket_path is not None,
            injected_client=client is not None,
        )

    def _create_client(self):
        """Construct a redis-py client for the selected connection mode.

        Pool, Sentinel, Unix socket, cluster, URL, and host/port modes are
        selected in that order. For a non-TLS ``redis://`` URL, wrapper SSL
        options are omitted so the URL controls transport security.
        """
        import redis

        redis_kwargs = self._client_kwargs()

        if self.connection_pool is not None:
            return redis.Redis(connection_pool=self.connection_pool)

        if self.sentinel_hosts:
            if not self.sentinel_service_name:
                raise ValueError("sentinel_service_name is required when sentinel_hosts is provided")
            sentinel = redis.sentinel.Sentinel(self.sentinel_hosts, **self.sentinel_kwargs)
            return sentinel.master_for(self.sentinel_service_name, **redis_kwargs)

        if self.unix_socket_path:
            return redis.Redis(unix_socket_path=self.unix_socket_path, db=self.db, **redis_kwargs)

        if self.cluster:
            if self.url:
                return redis.RedisCluster.from_url(self.url, **redis_kwargs)
            return redis.RedisCluster(host=self.host, port=self.port, **redis_kwargs)

        if self.url:
            url_kwargs = dict(redis_kwargs)
            # redis.Redis.from_url() already encodes transport settings from the
            # URL scheme. Forwarding wrapper-level SSL arguments here triggers
            # redis-py to build an SSLConnection with unexpected kwargs.
            url_kwargs.pop("ssl", None)
            url_kwargs.pop("ssl_cert_reqs", None)
            return redis.Redis.from_url(self.url, **url_kwargs)

        return redis.Redis(host=self.host, port=self.port, db=self.db, **redis_kwargs)

    def _client_kwargs(self) -> dict[str, Any]:
        """Merge redis-py options and omit values that were not configured.

        Wrapper fields override duplicate values from ``client_kwargs``.
        Optional protocol and client-name fields are added only when set.

        Returns:
            Keyword arguments passed to the selected redis-py constructor.
        """
        kwargs = dict(self.client_kwargs)
        kwargs.update(
            username=self.username,
            password=self.password,
            credential_provider=self.credential_provider,
            ssl=self.ssl,
            ssl_cert_reqs=self.ssl_cert_reqs,
            decode_responses=self.decode_responses,
            socket_timeout=self.socket_timeout,
            socket_connect_timeout=self.socket_connect_timeout,
            health_check_interval=self.health_check_interval,
        )

        if self.protocol is not None:
            kwargs["protocol"] = self.protocol
        if self.client_name:
            kwargs["client_name"] = self.client_name

        return {key: value for key, value in kwargs.items() if value is not None}


class KafkaConnection(Lattice):
    """
    Configure Kafka-compatible admin, producer, and consumer clients.

    This wrapper uses ``confluent-kafka`` for all three client types. Each
    component can be injected independently or created from its corresponding
    ``create_*`` option. Supplying ``client`` bypasses all construction and
    stores the explicitly supplied component clients as-is. Otherwise,
    ``client`` is set to the first available producer, consumer, or admin
    client. Consumer topics are subscribed during consumer creation.

    Args:
        provider: Service label: ``kafka``, ``redpanda``, ``msk``, or
            ``confluent``.
        bootstrap_servers: Broker address or list of broker addresses.
        client_id: Optional client identifier.
        create_admin_client: Whether to create an admin client if none injected.
        create_producer: Whether to create a producer if none injected.
        create_consumer: Whether to create a consumer if none injected.
        consumer_topics: Topics subscribed to by a created consumer.
        group_id: Consumer group identifier.
        security_protocol: Broker security protocol.
        sasl_mechanism: Optional SASL mechanism.
        sasl_username: Optional SASL username.
        sasl_password: Optional SASL password.
        sasl_oauth_token_provider: Optional OAuth token callback for Kafka.
        oauth_cb: Optional OAuth callback passed to librdkafka.
        ssl_cafile: Optional CA certificate path.
        ssl_certfile: Optional client certificate path.
        ssl_keyfile: Optional client private-key path.
        ssl_password: Optional password for the client private key.
        request_timeout_ms: Optional request timeout mapped to librdkafka's
            ``request.timeout.ms`` setting.
        admin_config: Admin-specific librdkafka options; these override shared
            client settings.
        producer_config: Producer-specific librdkafka options.
        consumer_config: Consumer-specific librdkafka options.
        client: Optional top-level injected client. When set, no components are
            constructed and the other injected component values are retained.
        admin_client: Optional injected admin client.
        producer: Optional injected producer.
        consumer: Optional injected consumer.
        residue: Optional shared logger; defaults to a lattice-scoped logger.
        **client_config: Shared librdkafka options overridden by component
            configuration dictionaries.

    Raises:
        ValueError: If ``provider`` is unsupported, or no client is injected
            and all component creation options are disabled.
        Exception: Client creation errors are logged and re-raised.

    Examples:

        KafkaConnection(bootstrap_servers="localhost:9092")

        KafkaConnection(
            provider="redpanda",
            bootstrap_servers="localhost:19092",
        )

        KafkaConnection(
            provider="confluent",
            bootstrap_servers="pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="confluent-api-key",
            sasl_password="confluent-api-secret",
        )

        KafkaConnection(
            provider="msk",
            bootstrap_servers=["b-1.cluster.kafka.us-east-1.amazonaws.com:9098"],
            security_protocol="SASL_SSL",
            sasl_mechanism="OAUTHBEARER",
            sasl_oauth_token_provider=msk_iam_token_provider,
        )

        KafkaConnection(
            bootstrap_servers="broker-1:9093",
            security_protocol="SSL",
            ssl_cafile="/path/to/ca.pem",
            ssl_certfile="/path/to/client.pem",
            ssl_keyfile="/path/to/client.key",
        )
    """

    supported_providers = {"kafka", "redpanda", "msk", "confluent"}

    def __init__(
        self,
        *,
        provider: str = "kafka",
        bootstrap_servers: str | list[str] | None = None,
        client_id: str | None = None,
        create_admin_client: bool = True,
        create_producer: bool = True,
        create_consumer: bool = False,
        consumer_topics: list[str] | None = None,
        group_id: str | None = None,
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        sasl_oauth_token_provider: Any = None,
        oauth_cb: Any = None,
        ssl_cafile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_keyfile: str | None = None,
        ssl_password: str | None = None,
        request_timeout_ms: int | None = None,
        admin_config: dict[str, Any] | None = None,
        producer_config: dict[str, Any] | None = None,
        consumer_config: dict[str, Any] | None = None,
        client: Any = None,
        admin_client: Any = None,
        producer: Any = None,
        consumer: Any = None,
        residue: Residue | None = None,
        **client_config: Any,
    ):
        """Validate the provider, create or inject clients, and log the outcome."""
        if provider not in self.supported_providers:
            raise ValueError(f"provider must be one of: {', '.join(sorted(self.supported_providers))}")
        if client is None and admin_client is None and producer is None and consumer is None and not create_admin_client and not create_producer and not create_consumer:
            raise ValueError("KafkaConnection needs create_admin_client, create_producer, create_consumer, or injected clients")

        self.provider = provider
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.create_admin_client = create_admin_client
        self.create_producer = create_producer
        self.create_consumer = create_consumer
        self.consumer_topics = consumer_topics or []
        self.group_id = group_id
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.sasl_username = sasl_username
        self.sasl_password = sasl_password
        self.sasl_oauth_token_provider = sasl_oauth_token_provider
        self.oauth_cb = oauth_cb
        self.ssl_cafile = ssl_cafile
        self.ssl_certfile = ssl_certfile
        self.ssl_keyfile = ssl_keyfile
        self.ssl_password = ssl_password
        self.request_timeout_ms = request_timeout_ms
        self.admin_config = admin_config or {}
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}
        self.client_config = client_config
        self.residue = (residue or Residue()).bind(component_name="lattice")

        try:
            if client is not None:
                self.client = client
                self.admin_client = admin_client
                self.producer = producer
                self.consumer = consumer
            else:
                self.admin_client = admin_client if admin_client is not None else self._create_admin_client() if create_admin_client else None
                self.producer = producer if producer is not None else self._create_producer() if create_producer else None
                self.consumer = consumer if consumer is not None else self._create_consumer() if create_consumer else None
                self.client = self.producer or self.consumer or self.admin_client
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "LATTICE: Failed to create Kafka connection",
                backend="kafka",
                provider=self.provider,
                bootstrap_servers=self._bootstrap_servers_string(),
                client_id=self.client_id,
                group_id=self.group_id,
                security_protocol=self.security_protocol,
                create_admin_client=self.create_admin_client,
                create_producer=self.create_producer,
                create_consumer=self.create_consumer,
                injected_client=client is not None,
                injected_admin_client=admin_client is not None,
                injected_producer=producer is not None,
                injected_consumer=consumer is not None,
                error=str(exc),
            )
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "LATTICE: Created Kafka connection",
            backend="kafka",
            provider=self.provider,
            bootstrap_servers=self._bootstrap_servers_string(),
            client_id=self.client_id,
            group_id=self.group_id,
            security_protocol=self.security_protocol,
            create_admin_client=self.create_admin_client,
            create_producer=self.create_producer,
            create_consumer=self.create_consumer,
            injected_client=client is not None,
            injected_admin_client=admin_client is not None,
            injected_producer=producer is not None,
            injected_consumer=consumer is not None,
        )

    def _create_admin_client(self):
        """Create a Confluent admin client using admin-specific options."""
        from importlib import import_module

        return import_module("confluent_kafka.admin").AdminClient(self._confluent_config(self.admin_config))

    def _create_producer(self):
        """Create a Confluent producer using producer-specific options."""
        from importlib import import_module

        return import_module("confluent_kafka").Producer(self._confluent_config(self.producer_config))

    def _create_consumer(self):
        """Create a Confluent consumer and subscribe it to configured topics."""
        from importlib import import_module

        consumer = import_module("confluent_kafka").Consumer(
            self._confluent_config(self.consumer_config, include_group=True)
        )
        if self.consumer_topics:
            consumer.subscribe(self.consumer_topics)
        return consumer

    def _confluent_config(
        self,
        overrides: dict[str, Any],
        *,
        include_group: bool = False,
    ) -> dict[str, Any]:
        """Build librdkafka options for one Kafka component.

        The wrapper's common settings form the base. The consumer group is
        included only when requested; shared client options then override the
        base, and component-specific overrides take final precedence. Unset
        values are removed.

        Returns:
            Configuration mapping accepted by a Confluent client constructor.
        """
        config = {
            "bootstrap.servers": self._bootstrap_servers_string(),
            "client.id": self.client_id,
            "security.protocol": self.security_protocol,
            "sasl.mechanisms": self.sasl_mechanism,
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
            "oauth_cb": self.oauth_cb,
            "ssl.ca.location": self.ssl_cafile,
            "ssl.certificate.location": self.ssl_certfile,
            "ssl.key.location": self.ssl_keyfile,
            "ssl.key.password": self.ssl_password,
            "request.timeout.ms": self.request_timeout_ms,
        }
        if include_group:
            config["group.id"] = self.group_id

        config.update(self.client_config)
        config.update(overrides)
        return {key: value for key, value in config.items() if value is not None}

    def _bootstrap_servers_string(self) -> str | None:
        """Return broker addresses in librdkafka's comma-separated format."""
        if isinstance(self.bootstrap_servers, list):
            return ",".join(self.bootstrap_servers)
        return self.bootstrap_servers




class OllamaConnection(Lattice):
    """
    Configure synchronous and optional asynchronous Ollama clients.

    A supplied synchronous client bypasses its construction. An async client
    is constructed only when requested and not supplied. The API key becomes
    a bearer Authorization header unless that exact header key is already
    present; additional client options are passed through to both constructors.

    Args:
        host: Optional Ollama server URL.
        api_key: Optional API key used to construct a bearer Authorization
            header.
        headers: Optional request headers; an explicit ``Authorization`` key
            takes precedence over ``api_key``.
        timeout: Optional request timeout.
        create_async_client: Whether to construct an async client when one is
            not injected.
        client: Optional preconfigured synchronous client.
        async_client: Optional preconfigured asynchronous client.
        residue: Optional shared logger; defaults to a lattice-scoped logger.
        **client_kwargs: Additional Ollama client options.

    Raises:
        Exception: Client creation errors are logged and re-raised.

    Examples:

        OllamaConnection()

        OllamaConnection(host="http://localhost:11434")

        OllamaConnection(
            host="https://ollama.example.com",
            api_key="ollama-api-key",
        )

        OllamaConnection(
            host="https://ollama.example.com",
            headers={"Authorization": "Bearer token"},
            timeout=60,
        )

        OllamaConnection(create_async_client=True)
    """

    def __init__(
        self,
        *,
        host: str | None = None,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        create_async_client: bool = False,
        client: Any = None,
        async_client: Any = None,
        residue: Residue | None = None,
        **client_kwargs: Any,
    ):
        """Create or inject requested clients and log the connection outcome."""
        self.host = host
        self.api_key = api_key
        self.headers = headers or {}
        self.timeout = timeout
        self.create_async_client = create_async_client
        self.client_kwargs = client_kwargs
        self.residue = (residue or Residue()).bind(component_name="lattice")

        try:
            self.client = client if client is not None else self._create_client()
            self.async_client = async_client if async_client is not None else self._create_async_client() if create_async_client else None
        except Exception as exc:
            self.residue.emit(
                self.residue.ash.ERROR,
                "LATTICE: Failed to create Ollama connection",
                backend="ollama",
                host=self.host,
                timeout=self.timeout,
                create_async_client=self.create_async_client,
                injected_client=client is not None,
                injected_async_client=async_client is not None,
                error=str(exc),
            )
            raise
        self.residue.emit(
            self.residue.ash.DEBUG,
            "LATTICE: Created Ollama connection",
            backend="ollama",
            host=self.host,
            timeout=self.timeout,
            create_async_client=self.create_async_client,
            injected_client=client is not None,
            injected_async_client=async_client is not None,
        )

    def _create_client(self):
        """Instantiate the synchronous client from the Ollama package."""
        from importlib import import_module

        return import_module("ollama").Client(**self._client_kwargs())

    def _create_async_client(self):
        """Instantiate the asynchronous client from the Ollama package."""
        from importlib import import_module

        return import_module("ollama").AsyncClient(**self._client_kwargs())

    def _client_kwargs(self) -> dict[str, Any]:
        """Build Ollama client options and omit values that were not configured.

        Host, headers, and timeout from the wrapper override duplicate values
        in ``client_kwargs``. If an API key is set and the exact
        ``Authorization`` header is absent, a bearer header is added.

        Returns:
            Keyword arguments passed to the synchronous or async client.
        """
        kwargs = dict(self.client_kwargs)
        headers = dict(self.headers)

        if self.api_key and "Authorization" not in headers:
            headers["Authorization"] = f"Bearer {self.api_key}"

        kwargs.update(
            host=self.host,
            headers=headers or None,
            timeout=self.timeout,
        )
        return {key: value for key, value in kwargs.items() if value is not None}

