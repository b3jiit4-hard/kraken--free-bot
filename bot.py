import os, json, math, csv
from datetime import datetime, timezone

import ccxt
import pandas as pd
import numpy as np
import yaml
import requests
from dateutil import tz
from strategy import EMACrossATR

def now_utc():
    return datetime.now(timezone.utc)

ROME = tz.gettz("Europe/Rome")
def ts_iso(dt):
    return dt.astimezone(ROME).strftime('%Y-%m-%d %H:%M:%S %Z')

class Notifier:
    def __init__(self):
        self.tok = os.getenv('TELEGRAM_TOKEN')
        self.chat = os.getenv('TELEGRAM_CHAT_ID')
    def send(self, msg: str):
        if not self.tok or not self.chat:
            return
        try:
            url = f"https://api.telegram.org/bot{self.tok}/sendMessage"
            requests.post(url, data={"chat_id": self.chat, "text": msg})
        except Exception:
            pass

class State:
    PATH = 'state.json'
    def __init__(self):
        if os.path.exists(self.PATH):
            with open(self.PATH, 'r') as f:
                self.data = json.load(f)
        else:
            self.data = {
                "equity": None,
                "cash_eur": None,
                "positions": {},
                "today": {"date": None, "pnl": 0.0, "trades": 0, "loss_streak": 0}
            }
    def save(self):
        with open(self.PATH, 'w') as f:
            json.dump(self.data, f, indent=2)
    def reset_if_new_day(self):
        today = now_utc().date().isoformat()
        if self.data['today']['date'] != today:
            self.data['today'] = {"date": today, "pnl": 0.0, "trades": 0, "loss_streak": 0}

class PaperBroker:
    def __init__(self, start_eur: float, fee_bps: int, slip_bps: int):
        self.cash = start_eur
        self.holdings = {}
        self.fee = fee_bps / 1e4
        self.slip = slip_bps / 1e4
    def balance_eur(self, prices):
        eq = self.cash
        for sym, qty in self.holdings.items():
            eq += qty * prices.get(sym, 0.0)
        return eq
    def buy(self, symbol, price, eur_notional):
        px = price * (1 + self.slip)
        fee = eur_notional * self.fee
        cost = eur_notional + fee
        if self.cash < cost:
            return 0.0, 0.0
        qty = eur_notional / px
        self.cash -= cost
        self.holdings[symbol] = self.holdings.get(symbol, 0.0) + qty
        return qty, px
    def sell(self, symbol, price, qty):
        pos = self.holdings.get(symbol, 0.0)
        if pos <= 0:
            return 0.0, 0.0
        qty = min(qty, pos)
        px = price * (1 - self.slip)
        eur = qty * px
        fee = eur * self.fee
        self.cash += (eur - fee)
        self.holdings[symbol] = pos - qty
        return qty, px

def load_config():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def fetch_ohlcv(ex, symbol, timeframe, limit=300):
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=['timestamp','open','high','low','close','volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
    return df

def main():
    cfg = load_config()
    notifier = Notifier()
    state = State()
    state.reset_if_new_day()

    PAPER = os.getenv('PAPER_TRADING', '1') == '1'
    ANALYSIS_ONLY = os.getenv('ANALYSIS_ONLY', '0') == '1'

    ex = ccxt.kraken({'enableRateLimit': True})
    ex.apiKey = os.getenv('KRAKEN_API_KEY')
    ex.secret = os.getenv('KRAKEN_API_SECRET')

    base_ccy = os.getenv('BASE_CCY', 'EUR')
    markets = ex.load_markets()

    if PAPER:
        start_cash = float(cfg.get('paper_start_eur', 100.0))
        if state.data['cash_eur'] is None:
            state.data['cash_eur'] = start_cash
        broker = PaperBroker(state.data['cash_eur'],
                             cfg.get('paper_fee_bps', 26),
                             cfg.get('paper_slippage_bps', 5))
        if state.data['positions']:
            for sym, pos in state.data['positions'].items():
                broker.holdings[sym] = pos.get('qty', 0.0)

    strat = EMACrossATR(cfg['ema_fast'], cfg['ema_slow'], cfg['atr_period'], cfg['atr_k'])

    md_bps = int(cfg.get('max_daily_drawdown_bps', 100))
    if state.data['today']['pnl'] <= -(md_bps/1e4) * (state.data['equity'] or cfg.get('paper_start_eur', 100.0)):
        print('Daily loss limit hit — skipping run')
        return

    utc_h = now_utc().hour
    s = cfg['session_utc']['start_hour']; e = cfg['session_utc']['end_hour']
    if not (s <= utc_h < e):
        print('Outside trading session window — skipping run')
        return

    prices = {}
    analysis_lines = []

    for symbol in cfg['symbols']:
        if symbol not in markets:
            print(f"Symbol {symbol} not in Kraken markets, skipping.")
            continue

        df = fetch_ohlcv(ex, symbol, cfg['timeframe'], limit=300)
        df = strat.compute(df)
        last = df.iloc[-1]; prev = df.iloc[-2]
        prices[symbol] = float(last['close'])

        cross_up = bool(last['ema_fast'] > last['ema_slow'] and prev['ema_fast'] <= prev['ema_slow'])
        cross_down = bool(last['ema_fast'] < last['ema_slow'] and prev['ema_fast'] >= prev['ema_slow'])
        analysis_lines.append(
            f"{symbol} px={prices[symbol]:.2f} | ema_fast={last['ema_fast']:.2f} ema_slow={last['ema_slow']:.2f} | signal={'LONG' if cross_up else ('EXIT' if cross_down else 'HOLD')}"
        )

        pos = state.data['positions'].get(symbol, {"qty": 0.0, "entry": 0.0, "stop": 0.0, "risked_eur": 0.0})
        in_pos = pos['qty'] > 0

        if ANALYSIS_ONLY:
            continue

        # EXIT
        should_exit = False; exit_reason = None
        if in_pos:
            if last['low'] <= pos['stop']:
                should_exit = True; exit_reason = 'stop'
            elif last['signal_exit']:
                should_exit = True; exit_reason = 'signal_exit'

        if should_exit:
            qty = pos['qty']
            if PAPER:
                sold_qty, px = broker.sell(symbol, last['close'], qty)
                pnl = (px - pos['entry']) * sold_qty
                state.data['cash_eur'] = broker.cash
            else:
                order = ex.create_order(symbol, 'market', 'sell', qty)
                px = float(order['average'] or last['close'])
                pnl = (px - pos['entry']) * qty
            state.data['today']['pnl'] += pnl
            state.data['today']['trades'] += 1
            state.data['positions'][symbol] = {"qty": 0.0, "entry": 0.0, "stop": 0.0, "risked_eur": 0.0}
            with open('fills.csv', 'a', newline='') as f:
                w = csv.writer(f); w.writerow([ts_iso(now_utc()), symbol, 'EXIT', qty, px, exit_reason, round(pnl, 2)])
            notifier.send(f"📤 EXIT {symbol} @ {round(px,2)} EUR | reason: {exit_reason} | PnL: {round(pnl,2)} EUR")

        # ENTRY
        if (not in_pos) and bool(last['signal_long']):
            stop_dist = float(last['stop_dist'])
            if stop_dist <= 0: continue
            equity_ref = state.data['equity'] or (broker.balance_eur(prices) if PAPER else None)
            if equity_ref is None and not PAPER:
                bal = ex.fetch_balance()
                equity_ref = bal.get(base_ccy, {}).get('total', 0.0) or 0.0
            bps = int(cfg.get('risk_per_trade_bps', 25))
            risk_eur = (bps/1e4) * float(equity_ref)
            qty = risk_eur / stop_dist

            mkt = markets[symbol]
            min_cost = (mkt.get('limits', {}).get('cost', {}) or {}).get('min', None)
            min_qty = (mkt.get('limits', {}).get('amount', {}) or {}).get('min', None)
            px = float(last['close'])
            notional = qty * px
            if min_cost and notional < min_cost: qty = (min_cost + 1e-9) / px
            if min_qty and qty < min_qty: qty = min_qty
            if qty * px < 1.0: continue

            stop_price = px - stop_dist

            if PAPER:
                bought_qty, fill_px = broker.buy(symbol, px, eur_notional=qty*px)
                if bought_qty <= 0: continue
                state.data['cash_eur'] = broker.cash
                pos = {"qty": bought_qty, "entry": fill_px, "stop": stop_price, "risked_eur": risk_eur}
            else:
                order = ex.create_order(symbol, 'market', 'buy', qty)
                fill_px = float(order['average'] or px)
                pos = {"qty": float(order['filled']), "entry": fill_px, "stop": stop_price, "risked_eur": risk_eur}

            state.data['positions'][symbol] = pos
            with open('fills.csv', 'a', newline='') as f:
                w = csv.writer(f); w.writerow([ts_iso(now_utc()), symbol, 'ENTRY', pos['qty'], pos['entry'], f"stop={round(stop_price,2)}", ''])
            notifier.send(f"📥 ENTRY {symbol} @ {round(pos['entry'],2)} EUR | stop {round(stop_price,2)} | size ~{round(pos['qty'],6)}")

    # Equity
    if PAPER:
        eq = broker.balance_eur({s: prices.get(s, 0.0) for s in cfg['symbols']})
        state.data['equity'] = eq
        state.data['cash_eur'] = broker.cash
    else:
        bal = ex.fetch_balance()
        eq = float(bal.get(base_ccy, {}).get('total', 0.0) or 0.0)
        for sym in cfg['symbols']:
            base, quote = sym.split('/')
            qty = float(bal.get(base, {}).get('total', 0.0) or 0.0)
            px = prices.get(sym, 0.0); eq += qty * px
        state.data['equity'] = eq

    state.save()
    if analysis_lines:
        notifier.send("🔎 Analisi: " + "\n".join(analysis_lines))
    print(f"Done at {ts_iso(now_utc())}. Equity: {round(state.data['equity'],2)} {base_ccy}")

if __name__ == '__main__':
    main()
