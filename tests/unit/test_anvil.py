import pytest

from oxidizer import Anvil


class FakeResponse:
	def __init__(self, payload=None, *, status_code=200, error=None):
		self.payload = payload
		self.status_code = status_code
		self.content = b"" if status_code == 204 else b"response"
		self.error = error

	def raise_for_status(self):
		if self.error:
			raise self.error

	def json(self):
		return self.payload


class FakeSession:
	def __init__(self, response=None):
		self.headers = {}
		self.response = response or FakeResponse({"ok": True})
		self.calls = []
		self.closed = False

	def get(self, url, params=None, *, allow_redirects):
		assert allow_redirects is False
		self.calls.append(("GET", url, params))
		return self.response

	def post(self, url, json=None, *, allow_redirects):
		assert allow_redirects is False
		self.calls.append(("POST", url, json))
		return self.response

	def close(self):
		self.closed = True


def test_anvil_configures_bearer_auth_and_get_request():
	session = FakeSession()
	anvil = Anvil(
		{
			"base_url": "https://api.example.test/v1/",
			"authentication": {"auth_type": "bearer", "credentials": {"token": "test-token"}},
		},
		session=session,
	)

	assert session.headers["Authorization"] == "Bearer test-token"
	assert session.headers["Content-Type"] == "application/json"
	assert anvil.get("/records", params={"limit": 2}) == {"ok": True}
	assert session.calls == [("GET", "https://api.example.test/v1/records", {"limit": 2})]


@pytest.mark.parametrize("token", [None, "", 123, True, ["token"], {"value": "token"},
	"token\rInjected: yes", "token\nInjected: yes", "token\r\nInjected: yes"])
def test_anvil_rejects_invalid_bearer_tokens_before_installing_header(token):
	session = FakeSession()
	with pytest.raises(ValueError):
		Anvil({"base_url": "https://api.example.test", "authentication": {
			"auth_type": "bearer", "credentials": {"token": token},
		}}, session=session)
	assert session.headers == {"Content-Type": "application/json"}
	assert session.calls == []


def test_anvil_configures_api_key_and_posts_json():
	session = FakeSession()
	anvil = Anvil(
		{"base_url": "https://api.example.test", "authentication": {
			"auth_type": "api_key", "credentials": {"key_name": "X-Api-Key", "key_value": "secret"},
		}},
		session=session,
	)
	payload = {"name": "sample"}

	assert session.headers["X-Api-Key"] == "secret"
	assert anvil.post("https://api.example.test/items", data=payload) == {"ok": True}
	assert session.calls == [("POST", "https://api.example.test/items", payload)]


def test_anvil_configures_multiple_authentication_headers():
	session = FakeSession()
	Anvil({"base_url": "https://data.alpaca.markets", "authentication": {
		"auth_type": "headers", "credentials": {"headers": {
			"APCA-API-KEY-ID": "test-key-id",
			"APCA-API-SECRET-KEY": "test-secret-key",
		}},
	}}, session=session)

	assert session.headers["APCA-API-KEY-ID"] == "test-key-id"
	assert session.headers["APCA-API-SECRET-KEY"] == "test-secret-key"


@pytest.mark.parametrize("headers", [
	{},
	{"APCA-API-KEY-ID": 123},
	{"APCA-API-KEY-ID": "bad\r\nInjected: yes"},
	{"APCA-API-KEY-ID\r\nInjected": "value"},
	{"Host": "attacker.example"},
	{"Content-Length": "0"},
])
def test_anvil_rejects_invalid_authentication_headers(headers):
	with pytest.raises(ValueError):
		Anvil({"base_url": "https://data.alpaca.markets", "authentication": {
			"auth_type": "headers", "credentials": {"headers": headers},
		}}, session=FakeSession())


@pytest.mark.parametrize("name,value", [
	("X-Api-Key\r\nInjected", "secret"),
	("X Api Key", "secret"),
	(123, "secret"),
	("X-Api-Key", "secret\rInjected: yes"),
	("X-Api-Key", "secret\nInjected: yes"),
	("X-Api-Key", ""),
	("X-Api-Key", 123),
	("X-Api-Key", None),
	("hOsT", "other.example"),
	("Content-Length", "0"),
	("Connection", "close"),
	("Proxy-Authorization", "secret"),
	("Transfer-Encoding", "chunked"),
])
@pytest.mark.parametrize("auth_type", ["api_key", "headers"])
def test_anvil_validates_auth_headers_before_installing_them(name, value, auth_type):
	session = FakeSession()
	credentials = (
		{"key_name": name, "key_value": value}
		if auth_type == "api_key" else {"headers": {name: value}}
	)
	with pytest.raises(ValueError):
		Anvil({"base_url": "https://api.example.test", "authentication": {
			"auth_type": auth_type, "credentials": credentials,
		}}, session=session)
	assert session.headers == {"Content-Type": "application/json"}
	assert session.calls == []


@pytest.mark.parametrize("endpoint", [
	"https://other.example.test/items",
	"http://api.example.test/items",
	"https://api.example.test:444/items",
	"https://api.example.test:0/items",
	"file:///etc/passwd",
	"https://user:password@api.example.test/items",
])
@pytest.mark.parametrize("method", ["get", "post"])
def test_anvil_rejects_unsafe_endpoints_before_sending_credentials(endpoint, method):
	session = FakeSession()
	anvil = Anvil({"base_url": "https://api.example.test", "authentication": {
		"auth_type": "api_key", "credentials": {"key_name": "X-Api-Key", "key_value": "test-secret"},
	}}, session=session)

	with pytest.raises(ValueError):
		getattr(anvil, method)(endpoint)

	assert session.calls == []


@pytest.mark.parametrize("status_code", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("method", ["get", "post"])
def test_anvil_does_not_follow_redirects(status_code, method):
	session = FakeSession(FakeResponse(status_code=status_code))
	anvil = Anvil({"base_url": "https://api.example.test"}, session=session)

	with pytest.raises(ValueError, match="redirects are disabled"):
		getattr(anvil, method)("/redirect")

	assert len(session.calls) == 1


def test_anvil_allows_explicit_default_port_on_the_same_origin():
	session = FakeSession()
	anvil = Anvil({"base_url": "https://api.example.test"}, session=session)

	assert anvil.get("https://api.example.test:443/items") == {"ok": True}


def test_anvil_requires_base_url_for_authenticated_absolute_requests():
	session = FakeSession()
	anvil = Anvil({"authentication": {
		"auth_type": "bearer", "credentials": {"token": "test-token"},
	}}, session=session)

	with pytest.raises(ValueError, match="require a base_url"):
		anvil.get("https://api.example.test/items")

	assert session.calls == []


def test_anvil_returns_none_for_empty_success_response():
	anvil = Anvil(session=FakeSession(FakeResponse(status_code=204)))

	assert anvil.post("https://api.example.test/no-content") is None


def test_anvil_propagates_http_errors():
	error = RuntimeError("request failed")
	anvil = Anvil(session=FakeSession(FakeResponse(error=error)))

	with pytest.raises(RuntimeError, match="request failed"):
		anvil.get("https://api.example.test/failure")


def test_anvil_rejects_unsupported_authentication():
	with pytest.raises(ValueError, match="Unsupported authentication type"):
		Anvil({"authentication": {"auth_type": "basic"}}, session=FakeSession())


def test_anvil_close_closes_session():
	session = FakeSession()
	Anvil(session=session).close()

	assert session.closed