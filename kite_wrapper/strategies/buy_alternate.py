"""Buy Alternate strategy — buys CE/PE, alternates option type on SL hit."""

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)


@register_strategy("buy_alternate")
class BuyAlternateStrategy(BaseStrategy):

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        option_type = strategy_data.get("option_type", "CE")
        return select_nifty_option(ctx.client, ctx.settings, option_type)

    def initial_direction(self, strategy_data: dict) -> str:
        strategy_data["option_type"] = "CE"
        return "BUY"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        # Toggle CE ↔ PE
        current = strategy_data.get("option_type", "CE")
        strategy_data["option_type"] = "PE" if current == "CE" else "CE"
        return {"action": "reselect_and_enter", "direction": "BUY"}
