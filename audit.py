import os, sys
from datetime import datetime, timezone
import ccxt
import pandas as pd
import yaml
import requests
from strategy import EMACrossATR

REPORT_PATH = 'audit_report.md'

OK = "✅"
WARN = "⚠️"
ERR = "❌"

def write_report(lines):
    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

def load_cfg():
    with open('config.yaml', 'r') as f:
        return yaml.safe_load(f)

def notify_telegram(msg: str):
    tok = os.getenv('TELEGRAM_TOKEN')
    chat = os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not chat:
        return
    try:
        url = f"https://api.telegram.org/bot{tok}/sendMessage"
        requests.post(url, data={"chat_id": chat, "text": msg})
    except Exception:
        pass

def main():
    cfg = load_cfg()
    base_ccy = os.getenv('BASE_CCY', 'EUR')

    ex = ccxt.kraken({'enableRateLimit': True})
    ex.apiKey = os.getenv('KRAKEN_API_KEY')
    ex.secret = os.getenv('KRAKEN_API_SECRET')

    passed = True
    lines = []
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    lines.append(f"# Pre-Flight Audit — {ts}\n")

    # 1) API sanity
    try:
        bal = ex.fetch_balance()
        lines.append(f"{OK} API key valida: fetch_balance ok.")
    except Exception as e:
        lines.append(f"{ERR} API key NON valida o permessi insufficienti: {e}")
        passed = False
        write_report(lines)
        notify_telegram("❌ Audit fallito: API non valide o permessi insufficienti.")
        print('API audit failed')
        sys.exit(1)

    # 2) Fee tier (best-effort)
    taker_bps = 26
    try:
        fees = ex.fetch_trading_fees() if hasattr(ex, 'fetch_trading_fees') else None
        if fees and isinstance(fees, dict):
            sample = fees.get('BTC/EUR') or (list(fees.values())[0] if fees else None)
            if isinstance(sample, dict) and 'taker' in sample:
                taker_bps = int(round(sample['taker'] * 1e4))
                lines.append(f"{OK} Fee taker ≈ {taker_bps/1e2:.2f}%.")
            else:
                lines.append(f"{WARN} Fee precise non disponibili; uso 0.26%.")
        else:
            lines.append(f"{WARN} API fee non disponibili; uso 0.26%.")
    except Exception as e:
        lines.append(f"{WARN} Lettura fee fallita ({e}); uso 0.26%.")

    # 3) Mercati
    markets = ex.load_markets()
    missing = [s for s in cfg['symbols'] if s not in markets]
    if missing:
        lines.append(f"{ERR} Coppie assenti su Kraken: {', '.join(missing)}")
        passed = False

    # 4) Saldi
    eur_total = float(bal.get(base_ccy, {}).get('total') or 0.0)
    lines.append(f"{OK} Saldo {base_ccy}: {eur_total:.2f} {base_ccy}.")

    # 5) Dry-run sizing/minimi
    strat = EMACrossATR(cfg['ema_fast'], cfg['ema_slow'], cfg['atr_period'], cfg['atr_k'])
    timeframe = cfg['timeframe']
    tradable_summary = []

    for sym in cfg['symbols']:
        if sym not in markets:
            continue
        mkt = markets[sym]
        try:
            ohlcv = ex.fetch_ohlcv(sym, timeframe=timeframe, limit=300)
            if not ohlcv or len(ohlcv) < 50:
                lines.append(f"{WARN} {sym}: dati OHLCV insufficienti.")
                continue
            df = pd.DataFrame(ohlcv, columns=['ts','open','high','low','close','volume'])
            df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
            df = strat.compute(df)
            last = df.iloc[-1]
            atr_stop = float(last['stop_dist'])
            px = float(last['close'])
            if atr_stop <= 0:
                lines.append(f"{WARN} {sym}: stop_dist <= 0.")
                continue
            risk_bps = int(cfg.get('risk_per_trade_bps', 25))
            equity_ref = eur_total if eur_total > 0 else float(cfg.get('paper_start_eur', 50.0))
            risk_eur = (risk_bps/1e4) * equity_ref
            qty = risk_eur / atr_stop
            notional = qty * px
            min_cost = (mkt.get('limits', {}).get('cost', {}) or {}).get('min')
            min_qty  = (mkt.get('limits', {}).get('amount', {}) or {}).get('min')
            reasons = []
            if min_cost and notional < float(min_cost):
                reasons.append(f"notional {notional:.2f} < min_cost {float(min_cost):.2f}")
            if min_qty and qty < float(min_qty):
                reasons.append(f"qty {qty:.8f} < min_qty {float(min_qty):.8f}")
            if notional < 1.0:
                reasons.append("notional < 1 EUR")
            if reasons:
                lines.append(f"{ERR} {sym}: NON tradabile — {', '.join(reasons)}")
                tradable_summary.append(f"{sym}: NON tradabile ({'; '.join(reasons)})")
                passed = False
            else:
                lines.append(f"{OK} {sym}: tradabile. qty≈{qty:.8f}, notional≈{notional:.2f} EUR, stop≈{atr_stop:.2f}.")
                tradable_summary.append(f"{sym}: tradabile qty≈{qty:.6f}, notional≈{notional:.2f} EUR")
        except Exception as e:
            lines.append(f"{ERR} {sym}: errore dati — {e}")
            tradable_summary.append(f"{sym}: errore ({e})")
            passed = False

    lines.append("\n---\n")
    lines.append(f"Esito complessivo: {'PASS' if passed else 'FAIL'}")

    write_report(lines)

    summary = f"📋 Audit {('PASS' if passed else 'FAIL')} — {ts}\n" + "\n".join(tradable_summary[:6])
    notify_telegram(summary)

    print('Wrote audit report')
    if not passed:
        sys.exit(2)

if __name__ == '__main__':
    main()
