"""Unit: redaction boundary for connector-owned surfaces (TC-35)."""

from __future__ import annotations

from okto_nexus_connector.redaction import redact_mapping, redact_text


def test_bearer_redacted():
    text = "Authorization: Bearer abcdef1234567890 sent"
    assert "abcdef1234567890" not in redact_text(text)
    assert "[redacted]" in redact_text(text)


def test_ticket_and_capability_shapes_redacted():
    assert "nstkt_secret1234" not in redact_text(
        "ticket=nstkt_secret1234 renew")
    assert "mcp-cap:rs_1" not in redact_text("ref mcp-cap:rs_1 used")
    assert "nxs_verylongcanonicalkey123" not in redact_text(
        "key nxs_verylongcanonicalkey123 leaked")


def test_extra_literals_redacted():
    out = redact_text("value XYZSECRET inline", extra=["XYZSECRET"])
    assert "XYZSECRET" not in out


def test_mapping_recursive():
    payload = {
        "url": "https://x", "headers": {"Authorization": "Bearer tok12345678"},
        "nested": [{"key": "nstkt_inner12345"}],
    }
    out = redact_mapping(payload)
    assert "tok12345678" not in str(out)
    assert "nstkt_inner12345" not in str(out)


def test_plain_data_untouched():
    assert redact_text("hello world 42") == "hello world 42"
