"""SAR (Stop-and-Reverse) strategy — sells NIFTY CE, reverses on SL hit."""

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)


@register_strategy("sar")
class SARStrategy(BaseStrategy):

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        return select_nifty_option(ctx.client, "CE", ctx.settings.min_premium,
                                       ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        # BULLISH → BUY CE (price goes up), BEARISH → SELL CE (price goes down)
        return "BUY" if bias == "BULLISH" else "SELL"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        new_dir = "BUY" if ctx.current_direction == "SELL" else "SELL"
        return {"action": "reselect_and_enter", "direction": new_dir}
