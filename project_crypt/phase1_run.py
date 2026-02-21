from __future__ import annotations

from .config import RiskConfig, load_env_config
from .models import AccountState, OrderProposal
from .risk_manager import RiskManager
from .store import RiskStore
from .telegram_notify import TelegramNotifier


def run_phase1_gate_check() -> bool:
    env = load_env_config()
    cfg = RiskConfig(kill_switch_file=env.kill_switch_file)
    store = RiskStore(cfg.db_path)
    rm = RiskManager(cfg, store)
    tg = TelegramNotifier(env.telegram_bot_token, env.telegram_chat_id)

    account = AccountState(
        equity=10000,
        daily_start_equity=10000,
        hourly_start_equity=10000,
        open_positions=0,
        consecutive_losses=0,
    )
    proposal = OrderProposal(
        symbol="BTC_USDT",
        side="buy",
        entry_price=95000,
        atr=500,
        expected_win_rate=0.57,
        expected_r_multiple=1.8,
        metadata={"mode": "paper", "source": "phase1-smoketest"},
    )

    decision = rm.evaluate(proposal, account)
    msg = (
        f"[Project Crypt][Phase-1] gate={decision.approved} reason={decision.reason} "
        f"symbol={proposal.symbol}"
    )
    print(msg)
    tg.send(msg)
    return decision.approved


if __name__ == "__main__":
    ok = run_phase1_gate_check()
    raise SystemExit(0 if ok else 2)
