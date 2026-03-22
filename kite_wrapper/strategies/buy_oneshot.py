"""One-shot Buy CE / Buy PE strategies — enter once, trail, exit on SL, stop engine."""

from ..base_strategy import (
    BaseStrategy,
    StrategyContext,
    register_strategy,
    select_nifty_option,
)


@register_strategy("buy_ce")
class BuyCEStrategy(BaseStrategy):

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        return select_nifty_option(ctx.client, "CE", ctx.settings.min_premium,
                                   ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        strategy_data["option_type"] = "CE"
        return "BUY"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        return {"action": "stop"}

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        return {"action": "trail"}


@register_strategy("buy_pe")
class BuyPEStrategy(BaseStrategy):

    def select_instrument(self, ctx: StrategyContext, strategy_data: dict) -> dict | None:
        return select_nifty_option(ctx.client, "PE", ctx.settings.min_premium,
                                   ctx.settings.expiry_type)

    def initial_direction(self, strategy_data: dict, bias: str) -> str:
        strategy_data["option_type"] = "PE"
        return "BUY"

    def on_sl_hit(self, ctx: StrategyContext, strategy_data: dict) -> dict:
        return {"action": "stop"}

    def on_target_hit(self, ctx: StrategyContext, strategy_data: dict,
                      lots_remaining: int) -> dict:
        return {"action": "trail"}
