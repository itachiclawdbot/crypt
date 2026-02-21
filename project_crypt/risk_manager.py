from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

from .config import RiskConfig
from .models import AccountState, GateDecision, OrderProposal
from .store import RiskStore


class RiskManager:
    """Phase-1 hard gate before any order execution path."""

    def __init__(self, config: RiskConfig, store: RiskStore) -> None:
        self.cfg = config
        self.store = store

    def evaluate(self, proposal: OrderProposal, account: AccountState) -> GateDecision:
        # 1) Kill switch hard-stop
        if Path(self.cfg.kill_switch_file).exists():
            return self._deny("kill-switch-file-detected", proposal, account)

        # 2) Circuit breakers
        daily_dd = self._drawdown(account.daily_start_equity, account.equity)
        if daily_dd >= self.cfg.daily_drawdown_limit:
            return self._deny(f"daily-dd-breach:{daily_dd:.4f}", proposal, account)

        hourly_dd = self._drawdown(account.hourly_start_equity, account.equity)
        if hourly_dd >= self.cfg.hourly_drawdown_limit:
            return self._deny(f"hourly-dd-breach:{hourly_dd:.4f}", proposal, account)

        if account.consecutive_losses >= self.cfg.max_consecutive_losses:
            return self._deny("consecutive-losses-breach", proposal, account)

        # 3) Position-count gate
        if account.open_positions >= 2:
            return self._deny("max-concurrent-positions-reached", proposal, account)

        # 4) Sizing + order payload
        sizing = self._position_size(proposal, account)
        if sizing["notional"] <= 0:
            return self._deny("invalid-size", proposal, account)

        payload = {
            "symbol": proposal.symbol,
            "side": proposal.side.upper(),
            "entry_price": proposal.entry_price,
            "stop_price": sizing["stop_price"],
            "size_notional": sizing["notional"],
            "kelly_fraction": sizing["effective_kelly_fraction"],
            "risk_amount": sizing["risk_amount"],
            "approved_at": account.timestamp.isoformat(),
            "meta": proposal.metadata,
        }

        self.store.log_risk_event(
            event_type="gate",
            approved=True,
            reason="approved",
            payload={"proposal": asdict(proposal), "account": asdict(account), "payload": payload},
        )
        return GateDecision(approved=True, reason="approved", order_payload=payload)

    def _deny(self, reason: str, proposal: OrderProposal, account: AccountState) -> GateDecision:
        self.store.log_risk_event(
            event_type="gate",
            approved=False,
            reason=reason,
            payload={"proposal": asdict(proposal), "account": asdict(account)},
        )
        return GateDecision(approved=False, reason=reason, order_payload=None)

    @staticmethod
    def _drawdown(start_equity: float, equity: float) -> float:
        if start_equity <= 0:
            return 0.0
        return max(0.0, (start_equity - equity) / start_equity)

    def _position_size(self, proposal: OrderProposal, account: AccountState) -> dict:
        stop_price = self._resolve_stop_price(proposal)
        per_unit_risk = abs(proposal.entry_price - stop_price)
        if per_unit_risk <= 0:
            return {
                "notional": 0.0,
                "stop_price": stop_price,
                "effective_kelly_fraction": 0.0,
                "risk_amount": 0.0,
            }

        risk_budget = account.equity * self.cfg.risk_per_trade_fraction

        # Full Kelly for R-multiple: f = p - (1-p)/b
        p = max(0.01, min(0.99, proposal.expected_win_rate))
        b = max(0.1, proposal.expected_r_multiple)
        full_kelly = p - ((1 - p) / b)
        quarter_kelly = max(0.0, full_kelly * self.cfg.quarter_kelly_fraction)

        kelly_cap_notional = account.equity * quarter_kelly
        hard_notional_cap = account.equity * self.cfg.max_notional_fraction

        risk_based_notional = (risk_budget / per_unit_risk) * proposal.entry_price
        target_notional = min(risk_based_notional, hard_notional_cap)
        if quarter_kelly > 0:
            target_notional = min(target_notional, kelly_cap_notional)

        return {
            "notional": max(0.0, target_notional),
            "stop_price": stop_price,
            "effective_kelly_fraction": quarter_kelly,
            "risk_amount": risk_budget,
        }

    def _resolve_stop_price(self, proposal: OrderProposal) -> float:
        if proposal.stop_price is not None and proposal.stop_price > 0:
            return proposal.stop_price

        if proposal.atr and proposal.atr > 0:
            atr_mult = 1.5
            if proposal.side.lower() == "buy":
                return proposal.entry_price - (proposal.atr * atr_mult)
            return proposal.entry_price + (proposal.atr * atr_mult)

        fallback = self.cfg.default_fallback_stop_fraction
        if proposal.side.lower() == "buy":
            return proposal.entry_price * (1 - fallback)
        return proposal.entry_price * (1 + fallback)
