"""Buy Scale-Out (Candle Close) — enters 3 lots, scales out at 5-min candle close,
exits when (high_since_entry - LTP) > sl_points.  No exchange SL order."""

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)


@register_strategy("buy_ce_pe_scale_out_candle_close")
class BuyScaleOutCandleStrategy(BaseStrategy):

    use_exchange_sl = False
    use_targets = False
    candle_sl_minutes = 5

    @property
    def lot_multiplier(self) -> int:
        return 3

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        option_type = strategy_data.get("option_type", "CE")
        return select_nifty_option(ctx.client, option_type, ctx.settings.min_premium,
                                   ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        if "option_type" not in strategy_data:
            strategy_data["option_type"] = "CE" if bias == "BULLISH" else "PE"
        return "BUY"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        current = strategy_data.get("option_type", "CE")
        strategy_data["option_type"] = "PE" if current == "CE" else "CE"
        return {"action": "reselect_and_enter", "direction": "BUY"}
