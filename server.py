"""
Gold Sentinel backend skeleton.

Replace get_live_gold_price() with a verified live XAU/USD market-data provider.
For production iPhone alerts, use a push provider (APNs/Web Push) from the server,
because iOS will not keep a web page executing continuously in the background.
"""
import time, math, random
from collections import deque

PRICES = deque(maxlen=300)

def get_live_gold_price():
    # DEMO ONLY. Replace with a real provider API call.
    last = PRICES[-1] if PRICES else 2395.0
    return max(1500.0, last + random.uniform(-1.0, 1.0))

def ema(values, period):
    if len(values) < period: return None
    k = 2/(period+1)
    e = sum(list(values)[:period])/period
    for v in list(values)[period:]:
        e = v*k + e*(1-k)
    return e

def rsi(values, period=14):
    if len(values) <= period: return None
    vals=list(values)
    gains=losses=0.0
    for i in range(len(vals)-period, len(vals)):
        d=vals[i]-vals[i-1]
        gains += max(d,0); losses += max(-d,0)
    if losses == 0: return 100
    rs=(gains/period)/(losses/period)
    return 100-(100/(1+rs))

def analyze():
    vals=list(PRICES)
    e20,e50=ema(vals,20),ema(vals,50)
    rv=rsi(vals)
    if None in (e20,e50,rv): return None
    bullish=e20>e50 and 52<=rv<=70
    bearish=e20<e50 and 30<=rv<=48
    if not (bullish or bearish): return None
    score=min(99, round(55+abs(rv-50)*1.4+abs(e20-e50)*3))
    if score<80: return None
    p=vals[-1]; side="BUY" if bullish else "SELL"; risk=3.0
    sl=p-risk if side=="BUY" else p+risk
    tp1=p+risk*1.8 if side=="BUY" else p-risk*1.8
    tp2=p+risk*2.5 if side=="BUY" else p-risk*2.5
    return dict(side=side,entry=round(p,2),sl=round(sl,2),tp1=round(tp1,2),tp2=round(tp2,2),score=score)

if __name__=="__main__":
    while True:
        PRICES.append(get_live_gold_price())
        signal=analyze()
        if signal:
            print("ALERT:", signal)
            # TODO: send server-side push notification here.
        time.sleep(5)
