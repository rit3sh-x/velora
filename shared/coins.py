from dataclasses import dataclass


@dataclass(frozen=True)
class CoinSpec:
    coin: str
    symbol: str
    display: str
    binance_pair: str
    nitter_query: str


COINS: tuple[CoinSpec, ...] = (
    CoinSpec("bitcoin",  "BTC",  "Bitcoin",   "BTCUSDT",  "bitcoin"),
    CoinSpec("ethereum", "ETH",  "Ethereum",  "ETHUSDT",  "ethereum"),
    CoinSpec("solana",   "SOL",  "Solana",    "SOLUSDT",  "solana"),
    CoinSpec("ripple",   "XRP",  "Ripple",    "XRPUSDT",  "xrp"),
    CoinSpec("binance",  "BNB",  "BNB",       "BNBUSDT",  "bnb"),
    CoinSpec("dogecoin", "DOGE", "Dogecoin",  "DOGEUSDT", "dogecoin"),
)

BY_COIN: dict[str, CoinSpec] = {c.coin: c for c in COINS}
BY_SYMBOL: dict[str, CoinSpec] = {c.symbol: c for c in COINS}
BY_BINANCE_PAIR: dict[str, CoinSpec] = {c.binance_pair: c for c in COINS}

SYMBOL_TO_COIN: dict[str, str] = {c.binance_pair: c.coin for c in COINS}
COIN_NAMES: tuple[str, ...] = tuple(c.coin for c in COINS)


def coin_or_404(coin: str) -> CoinSpec:
    spec = BY_COIN.get(coin.lower())
    if spec is None:
        raise KeyError(coin)
    return spec
