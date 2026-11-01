from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

from oxidizer.core.residue import Ash, Residue


class Anvil:
	"""Send JSON HTTP requests using a named API connection configuration.

	The adapter reuses one requests-compatible session, applies configured
	authentication headers once, and resolves relative endpoints against the
	connection's ``base_url``. Responses are returned as decoded JSON, except
	for empty or ``204`` responses, which return ``None``.
	Requests stay on the configured base origin and do not follow redirects.
	"""

	def __init__(
		self,
		connection_details: dict[str, Any] | None = None,
		*,
		session: Any = None,
		residue: Residue | None = None,
	):
		"""Create an API client and apply connection-level settings.

		Args:
			connection_details: Mapping with optional ``base_url`` and
				``authentication`` settings.
			session: Optional requests-compatible session. When omitted, a new
				``requests.Session`` is created.
			residue: Optional logger used for authentication diagnostics.

		Raises:
			TypeError: If connection or authentication settings have the wrong shape.
			ValueError: If the URL scheme or authentication configuration is
				unsupported or incomplete.
			ImportError: If requests is unavailable and no session was injected.
		"""
		self.residue = (residue or Residue()).bind(component_name="anvil")
		if connection_details is not None and not isinstance(connection_details, dict):
			raise TypeError("connection_details must be a dictionary")
		self.connection_details = connection_details or {}
		self.base_url = self.connection_details.get("base_url", "").rstrip("/")
		if self.base_url and urlparse(self.base_url).scheme not in ("http", "https"):
			raise ValueError("base_url must use http or https")
		if session is None:
			try:
				import requests
			except ImportError as exc:
				raise ImportError("Install oxidizer-lite[anvil] to use the Anvil HTTP adapter") from exc
			session = requests.Session()
		self.session = session
		self.session.headers.update({"Content-Type": "application/json"})
		self._setup_connection()

	def _setup_connection(self) -> None:
		"""Validate auth settings and install authentication headers.

		Bearer, API-key, and explicit header modes are supported. Credentials are
		installed on the HTTP session and are not included in the authentication
		log event.

		Raises:
			ValueError: If authentication has an unsupported mode or missing
			required credential fields.
		"""
		authentication = self.connection_details.get("authentication")
		if not authentication:
			return
		if not isinstance(authentication, dict):
			raise ValueError("authentication must be a dictionary")
		auth_type = authentication.get("auth_type")
		credentials = authentication.get("credentials", {})
		if not isinstance(credentials, dict):
			raise ValueError("authentication credentials must be a dictionary")
		if auth_type == "bearer":
			token = credentials.get("token")
			if not token:
				raise ValueError("Bearer authentication requires credentials.token")
			self.session.headers.update({"Authorization": f"Bearer {token}"})
		elif auth_type == "api_key":
			key_name = credentials.get("key_name")
			key_value = credentials.get("key_value")
			if not key_name or key_value is None:
				raise ValueError("API-key authentication requires key_name and key_value")
			self.session.headers.update({key_name: key_value})
		elif auth_type == "headers":
			headers = credentials.get("headers")
			if not isinstance(headers, dict) or not headers:
				raise ValueError("Header authentication requires a non-empty credentials.headers mapping")
			reserved = {"connection", "content-length", "host", "proxy-authorization", "transfer-encoding"}
			for name, value in headers.items():
				if not isinstance(name, str) or not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
					raise ValueError("Authentication header names must be valid HTTP tokens")
				if name.lower() in reserved:
					raise ValueError(f"Authentication header {name!r} cannot override a transport header")
				if not isinstance(value, str) or not value or any(character in value for character in ("\r", "\n")):
					raise ValueError("Authentication header values must be non-empty strings without line breaks")
			self.session.headers.update(headers)
		else:
			raise ValueError(f"Unsupported authentication type: {auth_type}")
		self.residue.emit(Ash.INFO, "Configured API authentication", auth_type=auth_type)

	def _url(self, endpoint: str) -> str:
		"""Resolve an HTTP endpoint and enforce the configured base origin.

		Args:
			endpoint: Absolute URL or relative API path.

		Returns:
			The absolute URL passed to the HTTP session.

		Raises:
			ValueError: If the endpoint is empty or relative without a base URL.
		"""
		if not isinstance(endpoint, str) or not endpoint:
			raise ValueError("endpoint must be a non-empty string")
		if urlparse(endpoint).scheme:
			resolved = endpoint
		elif self.base_url:
			resolved = urljoin(f"{self.base_url}/", endpoint.lstrip("/"))
		else:
			raise ValueError("An absolute endpoint or base_url is required")
		parsed = urlparse(resolved)
		if parsed.scheme not in {"http", "https"} or not parsed.hostname:
			raise ValueError("endpoint must use http or https with a hostname")
		if parsed.username is not None or parsed.password is not None:
			raise ValueError("endpoint must not contain URL credentials")
		if self.base_url:
			base = urlparse(self.base_url)
			port = parsed.port if parsed.port is not None else (443 if parsed.scheme == "https" else 80)
			base_port = base.port if base.port is not None else (443 if base.scheme == "https" else 80)
			origin = (parsed.scheme, parsed.hostname, port)
			base_origin = (base.scheme, base.hostname, base_port)
			if origin != base_origin:
				raise ValueError("endpoint must use the same origin as base_url")
		elif self.connection_details.get("authentication"):
			raise ValueError("Authenticated API connections require a base_url")
		return resolved

	def get(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
		"""Send a GET request and decode its successful response.

		Args:
			endpoint: Absolute URL or path relative to ``base_url``.
			params: Optional query-string parameters.

		Returns:
			Decoded JSON body, or ``None`` for status 204 or an empty body.

		Raises:
			HTTP errors from the session, or JSON decoding errors for invalid
			non-empty response bodies.
		"""
		response = self.session.get(self._url(endpoint), params=params, allow_redirects=False)
		return self._handle_response(response)

	def post(self, endpoint: str, data: Any = None) -> Any:
		"""Send a JSON POST request and decode its successful response.

		Args:
			endpoint: Absolute URL or path relative to ``base_url``.
			data: Value serialized as the JSON request body.

		Returns:
			Decoded JSON body, or ``None`` for status 204 or an empty body.

		Raises:
			HTTP errors from the session, or JSON decoding errors for invalid
			non-empty response bodies.
		"""
		response = self.session.post(self._url(endpoint), json=data, allow_redirects=False)
		return self._handle_response(response)

	def _handle_response(self, response: Any) -> Any:
		"""Raise for HTTP errors and decode a non-empty response as JSON.

		Args:
			response: Requests-compatible response object.

		Returns:
			Decoded JSON body, or ``None`` for no-content/empty responses.
		"""
		if 300 <= response.status_code < 400:
			raise ValueError("API redirects are disabled; configure the final endpoint explicitly")
		response.raise_for_status()
		if response.status_code == 204 or not response.content:
			return None
		return response.json()

	def close(self) -> None:
		"""Close the underlying HTTP session and release its connections."""
		self.session.close()
