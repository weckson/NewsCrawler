"""Tests for the conservative noise filter + minimum-retention floor.

Design intent (do not loosen without re-checking recall on real headlines):
  - is_noise() only flags obvious hype / engagement bait, never real events.
  - filter_noise_with_floor() drops noise BUT guarantees a per-ticker minimum
    so a sparsely-covered ticker is never zeroed out by the filter.
"""

import crawl_news as cn
from crawl_news import is_noise, filter_noise_with_floor


# ── is_noise: catches obvious fluff ──────────────────────────────────────────

def test_flags_hype_adjectives():
    assert is_noise("Here's What Makes AMAT an Unstoppable Technology Stock")
    assert is_noise("Applied Materials Is Attracting Investor Attention")
    assert is_noise("3 No-Brainer Stocks to Buy Right Now")  # 'reasons/ways to buy' sib
    assert is_noise("This Millionaire-Maker Stock Could Set You Up for Life")


def test_flags_engagement_bait():
    assert is_noise("Here's What Makes Nvidia Stock a Buy")
    assert is_noise("5 Reasons to Buy This Chip Stock")
    assert is_noise("Why Wall Street Is Talking About This AI Stock")


# ── is_noise: must NOT flag real news (recall guard) ─────────────────────────

def test_does_not_flag_real_events():
    real = [
        "Applied Materials surges as it unveils new chipmaking systems",
        "Applied Materials (AMAT) Upgraded to Strong Buy: What Does It Mean",
        "Pfizer receives FDA approval for new cancer drug",
        "BigCo agrees to acquire SmallCo for $12 billion",
        "Nvidia beats Q3 estimates, raises full-year guidance",
        "Tesla recalls 50,000 vehicles over brake defect",
        "Apple names new CFO effective January",
        "Magnificent Seven stocks lead market higher",  # 'magnificent' is a real term
    ]
    for title in real:
        assert not is_noise(title), f"false positive on: {title}"


# ── filter_noise_with_floor: retention guarantees ────────────────────────────

def _a(title, q=0.5):
    return {"title": title, "_quality_score": q}


def test_drops_noise_when_plenty_of_signal():
    arts = [
        _a("Nvidia beats Q3 estimates, raises guidance", 0.8),
        _a("Nvidia agrees to acquire Xilinx for $40B", 0.8),
        _a("Nvidia announces new data center GPU", 0.7),
        _a("Here's What Makes Nvidia an Unstoppable Stock", 0.6),  # noise
    ]
    out = filter_noise_with_floor(arts, min_retain=2)
    titles = [a["title"] for a in out]
    assert "Here's What Makes Nvidia an Unstoppable Stock" not in titles
    assert len(out) == 3


def test_sparse_ticker_below_floor_is_untouched():
    # <= min_retain items → skip filtering entirely, even if all noise.
    arts = [_a("This Unstoppable Stock Is a No-Brainer Buy", 0.5)]
    out = filter_noise_with_floor(arts, min_retain=2)
    assert len(out) == 1  # kept despite being noise


def test_floor_restores_best_dropped_when_all_noise():
    # 3 items, all noise → filter would zero it, but floor=2 restores the
    # 2 highest-scoring ones.
    arts = [
        _a("Unstoppable Stock to Buy", 0.30),
        _a("No-Brainer Stock for Millionaires", 0.55),
        _a("Screaming Buy Stock Right Now", 0.45),
    ]
    out = filter_noise_with_floor(arts, min_retain=2)
    assert len(out) == 2
    scores = sorted(a["_quality_score"] for a in out)
    assert scores == [0.45, 0.55]  # kept the two best


def test_preserves_input_order():
    arts = [
        _a("Nvidia reports earnings", 0.8),
        _a("Unstoppable Stock to Buy", 0.6),  # noise, dropped
        _a("Nvidia launches new GPU", 0.7),
    ]
    out = filter_noise_with_floor(arts, min_retain=2)
    assert [a["title"] for a in out] == [
        "Nvidia reports earnings",
        "Nvidia launches new GPU",
    ]


def test_disabled_flag_returns_input_unchanged(monkeypatch):
    monkeypatch.setattr(cn, "NOISE_FILTER_ENABLED", False)
    arts = [
        _a("Nvidia earnings", 0.8),
        _a("Unstoppable Stock to Buy", 0.6),
        _a("Screaming Buy Now", 0.5),
    ]
    out = filter_noise_with_floor(arts, min_retain=2)
    assert len(out) == 3  # nothing dropped
