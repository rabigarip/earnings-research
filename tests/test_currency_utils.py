from src.utils.currency import convert, get_fx_rate


class _DummyTicker:
    def __init__(self, info):
        self.info = info


def test_get_fx_rate_same_currency():
    assert get_fx_rate("SAR", "SAR") == 1.0


def test_convert_uses_fallback_when_yahoo_unavailable(monkeypatch):
    def _boom(_):
        raise RuntimeError("network down")

    import src.utils.currency as cur
    monkeypatch.setattr(cur.yf, "Ticker", _boom)
    # SARUSD fallback = 0.2667
    out = convert(100.0, "SAR", "USD")
    assert out is not None
    assert round(out, 2) == 26.67

