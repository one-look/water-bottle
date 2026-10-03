from src.auth.domain.entities import Principal
from src.auth.exceptions import InvalidTokenError
from src.auth.infrastructure.jwt import JwtAccessTokenIssuer


def test_issue_and_verify_access_token():
    private_pem, public_pem = JwtAccessTokenIssuer.generate_keypair()
    issuer = JwtAccessTokenIssuer(
        private_key_pem=private_pem,
        public_key_pem=public_pem,
        issuer="http://test",
        audience="water-bottle",
    )
    principal = Principal(
        sub="user@example.com",
        tenant_id="nmc",
        client_id="test-client",
        role="student",
    )
    token = issuer.issue_access_token(principal, 300)
    decoded = issuer.verify_access_token(token)
    assert decoded.sub == principal.sub
    assert decoded.tenant_id == principal.tenant_id
    assert decoded.role == principal.role


def test_jwks_contains_public_key():
    private_pem, public_pem = JwtAccessTokenIssuer.generate_keypair()
    issuer = JwtAccessTokenIssuer(
        private_key_pem=private_pem,
        public_key_pem=public_pem,
        issuer="http://test",
        audience="water-bottle",
    )
    jwks = issuer.jwks_document()
    assert len(jwks["keys"]) == 1
    assert jwks["keys"][0]["kty"] == "RSA"
    assert jwks["keys"][0]["alg"] == "RS256"


def test_verify_rejects_tampered_token():
    private_pem, public_pem = JwtAccessTokenIssuer.generate_keypair()
    issuer = JwtAccessTokenIssuer(
        private_key_pem=private_pem,
        public_key_pem=public_pem,
        issuer="http://test",
        audience="water-bottle",
    )
    principal = Principal(sub="a", tenant_id="nmc", client_id="c", role=None)
    token = issuer.issue_access_token(principal, 300)
    broken = token[:-1] + ("a" if token[-1] != "a" else "b")
    try:
        issuer.verify_access_token(broken)
        assert False, "expected InvalidTokenError"
    except InvalidTokenError:
        pass
