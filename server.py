import json, os, threading, time, urllib.parse, urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_KEY=os.getenv("TWELVE_DATA_API_KEY","").strip()
SYMBOL=os.getenv("GOLD_SYMBOL","XAU/USD")
INTERVAL=os.getenv("GOLD_INTERVAL","5min")
SCAN_SECONDS=max(30,int(os.getenv("SCAN_SECONDS","60")))
MIN_SCORE=max(50,min(99,int(os.getenv("MIN_SCORE","80"))))
PORT=int(os.getenv("PORT","10000"))
URL="https://api.twelvedata.com/time_series"

lock=threading.Lock()
state={"service":"Gold Sentinel","mode":"LIVE" if API_KEY else "WAITING_FOR_API_KEY",
       "symbol":SYMBOL,"interval":INTERVAL,"last_update_utc":None,"last_price":None,
       "trend":None,"rsi":None,"atr":None,"score":None,"decision":"STARTING",
       "signal":None,"error":None}

def fetch_candles():
    if not API_KEY: raise RuntimeError("TWELVE_DATA_API_KEY is not set in Render.")
    qs=urllib.parse.urlencode({"symbol":SYMBOL,"interval":INTERVAL,"outputsize":120,"format":"JSON","apikey":API_KEY})
    req=urllib.request.Request(URL+"?"+qs,headers={"User-Agent":"Gold-Sentinel/1.0"})
    with urllib.request.urlopen(req,timeout=20) as r:
        p=json.loads(r.read().decode())
    if p.get("status")=="error": raise RuntimeError(p.get("message","Twelve Data error"))
    vals=p.get("values")
    if not vals: raise RuntimeError("No candle data returned. Check your Twelve Data plan supports XAU/USD.")
    return [{"t":x.get("datetime"),"o":float(x["open"]),"h":float(x["high"]),"l":float(x["low"]),"c":float(x["close"])} for x in reversed(vals)]

def ema(v,p):
    if len(v)<p:return None
    e=sum(v[:p])/p;k=2/(p+1)
    for x in v[p:]: e=x*k+e*(1-k)
    return e

def rsi(v,p=14):
    if len(v)<=p:return None
    g=l=0.0
    for i in range(len(v)-p,len(v)):
        d=v[i]-v[i-1]
        if d>0:g+=d
        else:l-=d
    if l==0:return 100.0
    rs=(g/p)/(l/p)
    return 100-(100/(1+rs))

def atr(c,p=14):
    if len(c)<=p:return None
    trs=[]
    for i in range(len(c)-p,len(c)):
        x=c[i];pc=c[i-1]["c"]
        trs.append(max(x["h"]-x["l"],abs(x["h"]-pc),abs(x["l"]-pc)))
    return sum(trs)/len(trs)

def analyze(c):
    closes=[x["c"] for x in c];e20=ema(closes,20);e50=ema(closes,50);rv=rsi(closes);av=atr(c)
    if None in (e20,e50,rv,av) or av<=0:return {"decision":"WAIT","signal":None}
    price=closes[-1];bull=e20>e50 and 52<=rv<=70;bear=e20<e50 and 30<=rv<=48
    score=int(round(min(99,50+min(25,abs(e20-e50)/av*10)+min(25,abs(rv-50)))))
    res={"price":round(price,2),"trend":"BULLISH" if e20>e50 else "BEARISH","rsi":round(rv,2),"atr":round(av,3),"score":score,"decision":"WAIT","signal":None}
    if score<MIN_SCORE or not (bull or bear):return res
    side="BUY" if bull else "SELL";risk=max(av*1.5,0.01)
    sl=price-risk if side=="BUY" else price+risk
    tp1=price+risk*1.8 if side=="BUY" else price-risk*1.8
    tp2=price+risk*2.5 if side=="BUY" else price-risk*2.5
    res["decision"]=side
    res["signal"]={"side":side,"entry":round(price,2),"stop_loss":round(sl,2),"take_profit_1":round(tp1,2),"take_profit_2":round(tp2,2),"score":score,"interval":INTERVAL}
    return res

def scan():
    global state
    while True:
        try:
            r=analyze(fetch_candles())
            with lock:
                state.update({"mode":"LIVE","last_update_utc":datetime.now(timezone.utc).isoformat(),"last_price":r.get("price"),"trend":r.get("trend"),"rsi":r.get("rsi"),"atr":r.get("atr"),"score":r.get("score"),"decision":r.get("decision"),"signal":r.get("signal"),"error":None})
            if r.get("signal"): print("SIGNAL",json.dumps(r["signal"]),flush=True)
        except Exception as e:
            with lock: state.update({"last_update_utc":datetime.now(timezone.utc).isoformat(),"decision":"ERROR","error":str(e)})
            print("Scanner error:",e,flush=True)
        time.sleep(SCAN_SECONDS)

class H(BaseHTTPRequestHandler):
    def sendj(self,obj,code=200):
        b=json.dumps(obj,indent=2).encode();self.send_response(code);self.send_header("Content-Type","application/json");self.send_header("Cache-Control","no-store");self.send_header("Access-Control-Allow-Origin","*");self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b)
    def do_GET(self):
        if self.path in ("/","/status"):
            with lock:s=dict(state)
            s["healthy"]=s["error"] is None;s["note"]="Live alert backend; no automatic trade execution."
            return self.sendj(s)
        if self.path=="/health":return self.sendj({"ok":True})
        return self.sendj({"error":"Not found"},404)
    def log_message(self,*a):pass

if __name__=="__main__":
    threading.Thread(target=scan,daemon=True).start()
    print("Gold Sentinel listening on",PORT,flush=True)
    ThreadingHTTPServer(("0.0.0.0",PORT),H).serve_forever()
