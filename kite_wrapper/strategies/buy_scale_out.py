"""Buy Scale-Out strategy — enters 3 lots, scales out on profit, switches CE/PE on SL."""

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)


@register_strategy("buy_scale_out")
class BuyScaleOutStrategy(BaseStrategy):

    @property
    def lot_multiplier(self) -> int:
        return 3

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        option_type = strategy_data.get("option_type", "CE")
        return select_nifty_option(ctx.client, ctx.settings, option_type)

    def initial_direction(self, strategy_data: dict) -> str:
        strategy_data["option_type"] = "CE"
        return "BUY"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        # Toggle CE <-> PE
        current = strategy_data.get("option_type", "CE")
        strategy_data["option_type"] = "PE" if current == "CE" else "CE"
        return {"action": "reselect_and_enter", "direction": "BUY"}

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        if lots_remaining > 1:
            return {"action": "partial_exit", "exit_lots": 1}
        return {"action": "trail"}
