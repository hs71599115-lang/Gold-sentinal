import json, os, threading, time, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

API_KEY=os.getenv('TWELVE_DATA_API_KEY','').strip()
SYMBOL=os.getenv('GOLD_SYMBOL','XAU/USD')
INTERVAL=os.getenv('GOLD_INTERVAL','5min')
CANDLE_REFRESH_SECONDS=max(240,int(os.getenv('CANDLE_REFRESH_SECONDS','300')))
QUOTE_REFRESH_SECONDS=max(30,int(os.getenv('QUOTE_REFRESH_SECONDS','60')))
ANALYSIS_SECONDS=max(5,int(os.getenv('ANALYSIS_SECONDS','10')))
MIN_SCORE=max(50,min(99,int(os.getenv('MIN_SCORE','80'))))
PORT=int(os.getenv('PORT','10000'))
TS_URL='https://api.twelvedata.com/time_series'
QUOTE_URL='https://api.twelvedata.com/quote'
BASE=Path(__file__).resolve().parent
INDEX=BASE/'index.html'
WORKER=BASE/'OneSignalSDKWorker.js'
lock=threading.Lock()
candles=[]
quote=None
last_candle=0.0
last_quote=0.0
candle_backoff=0.0
quote_backoff=0.0
state={'service':'Gold Sentinel','mode':'LIVE' if API_KEY else 'WAITING_FOR_API_KEY','symbol':SYMBOL,'interval':INTERVAL,'last_update_utc':None,'last_price':None,'trend':None,'rsi':None,'atr':None,'score':None,'decision':'STARTING','signal':None,'error':None,'data_source':None,'last_candle_fetch_utc':None,'last_quote_fetch_utc':None}

def api_json(url,params):
    if not API_KEY: raise RuntimeError('TWELVE_DATA_API_KEY is not set in Render.')
    q=dict(params); q['apikey']=API_KEY
    req=urllib.request.Request(url+'?'+urllib.parse.urlencode(q),headers={'User-Agent':'Gold-Sentinel/2.0'})
    try:
        with urllib.request.urlopen(req,timeout=20) as r: p=json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        if e.code==429: raise RuntimeError('RATE_LIMIT_429')
        raise
    if isinstance(p,dict) and p.get('status')=='error':
        m=p.get('message','Twelve Data error')
        if 'limit' in m.lower() or 'too many' in m.lower(): raise RuntimeError('RATE_LIMIT_429')
        raise RuntimeError(m)
    return p

def fetch_candles():
    p=api_json(TS_URL,{'symbol':SYMBOL,'interval':INTERVAL,'outputsize':120,'format':'JSON'})
    vals=p.get('values') if isinstance(p,dict) else None
    if not vals: raise RuntimeError('No candle data returned.')
    return [{'t':x.get('datetime'),'o':float(x['open']),'h':float(x['high']),'l':float(x['low']),'c':float(x['close'])} for x in reversed(vals)]

def fetch_quote():
    p=api_json(QUOTE_URL,{'symbol':SYMBOL,'format':'JSON'})
    for k in ('close','price'):
        if k in p and p[k] not in (None,''): return float(p[k])
    raise RuntimeError('Quote response did not contain a usable price.')

def ema(v,p):
    if len(v)<p:return None
    e=sum(v[:p])/p; k=2/(p+1)
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
    rs=(g/p)/(l/p); return 100-(100/(1+rs))

def atr(c,p=14):
    if len(c)<=p:return None
    trs=[]
    for i in range(len(c)-p,len(c)):
        x=c[i]; pc=c[i-1]['c']; trs.append(max(x['h']-x['l'],abs(x['h']-pc),abs(x['l']-pc)))
    return sum(trs)/len(trs)

def apply_quote(c,q):
    w=[dict(x) for x in c]
    if not w or q is None:return w
    w[-1]['c']=q; w[-1]['h']=max(w[-1]['h'],q); w[-1]['l']=min(w[-1]['l'],q)
    return w

def analyze(c):
    closes=[x['c'] for x in c]; e20=ema(closes,20); e50=ema(closes,50); rv=rsi(closes); av=atr(c)
    if None in (e20,e50,rv,av) or av<=0:return {'decision':'WAIT','signal':None}
    price=closes[-1]; bull=e20>e50 and 52<=rv<=70; bear=e20<e50 and 30<=rv<=48
    score=int(round(min(99,50+min(25,abs(e20-e50)/av*10)+min(25,abs(rv-50)))))
    res={'price':round(price,2),'trend':'BULLISH' if e20>e50 else 'BEARISH','rsi':round(rv,2),'atr':round(av,3),'score':score,'decision':'WAIT','signal':None}
    if score<MIN_SCORE or not (bull or bear):return res
    side='BUY' if bull else 'SELL'; risk=max(av*1.5,0.01)
    sl=price-risk if side=='BUY' else price+risk; tp1=price+risk*1.8 if side=='BUY' else price-risk*1.8; tp2=price+risk*2.5 if side=='BUY' else price-risk*2.5
    res['decision']=side; res['signal']={'side':side,'entry':round(price,2),'stop_loss':round(sl,2),'take_profit_1':round(tp1,2),'take_profit_2':round(tp2,2),'score':score,'interval':INTERVAL}
    return res

def candle_worker():
    global candles,last_candle,candle_backoff
    while True:
        now=time.time()
        if (not candles or now-last_candle>=CANDLE_REFRESH_SECONDS) and now>=candle_backoff:
            try:
                new=fetch_candles()
                with lock:
                    candles=new; state['last_candle_fetch_utc']=datetime.now(timezone.utc).isoformat()
                last_candle=now
            except Exception as e:
                if str(e)=='RATE_LIMIT_429': candle_backoff=now+300; msg='Twelve Data rate limit reached; candle history backing off 5 minutes.'
                else: msg=f'Candle fetch error: {e}'
                with lock: state['error']=msg
        time.sleep(5)

def quote_worker():
    global quote,last_quote,quote_backoff
    while True:
        now=time.time()
        if (quote is None or now-last_quote>=QUOTE_REFRESH_SECONDS) and now>=quote_backoff:
            try:
                q=fetch_quote()
                with lock:
                    quote=q; state['last_quote_fetch_utc']=datetime.now(timezone.utc).isoformat()
                last_quote=now
            except Exception as e:
                if str(e)=='RATE_LIMIT_429': quote_backoff=now+120; msg='Twelve Data rate limit reached; live quote backing off temporarily.'
                else: msg=f'Quote fetch error: {e}'
                with lock: state['error']=msg
        time.sleep(5)

def analysis_worker():
    while True:
        with lock: c=[dict(x) for x in candles]; q=quote
        if c:
            r=analyze(apply_quote(c,q))
            with lock:
                state.update({'mode':'LIVE','last_update_utc':datetime.now(timezone.utc).isoformat(),'last_price':r.get('price'),'trend':r.get('trend'),'rsi':r.get('rsi'),'atr':r.get('atr'),'score':r.get('score'),'decision':r.get('decision'),'signal':r.get('signal'),'data_source':'cached_5m_candles+live_quote' if q is not None else 'cached_5m_candles'})
                if state['last_price'] is not None: state['error']=None
            if r.get('signal'): print('SIGNAL',json.dumps(r['signal']),flush=True)
        time.sleep(ANALYSIS_SECONDS)

class H(BaseHTTPRequestHandler):
    def sendb(self,b,ct,code=200):
        self.send_response(code); self.send_header('Content-Type',ct); self.send_header('Cache-Control','no-store'); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def sendj(self,o,code=200): self.sendb(json.dumps(o,indent=2).encode(),'application/json; charset=utf-8',code)
    def do_GET(self):
        p=self.path.split('?',1)[0]
        if p=='/':
            if not INDEX.exists():return self.sendj({'error':'index.html not found'},500)
            return self.sendb(INDEX.read_bytes(),'text/html; charset=utf-8')
        if p=='/status':
            with lock:s=dict(state)
            s['healthy']=s['last_price'] is not None and s['error'] is None
            s['polling']={'quote_seconds':QUOTE_REFRESH_SECONDS,'candle_seconds':CANDLE_REFRESH_SECONDS,'analysis_seconds':ANALYSIS_SECONDS}
            s['note']='Smart polling: cached M5 candles plus live quote. No automatic trade execution.'
            return self.sendj(s)
        if p=='/OneSignalSDKWorker.js':
            if not WORKER.exists():return self.sendj({'error':'OneSignalSDKWorker.js not found'},404)
            return self.sendb(WORKER.read_bytes(),'application/javascript; charset=utf-8')
        if p=='/health':return self.sendj({'ok':True})
        return self.sendj({'error':'Not found'},404)
    def log_message(self,*a):pass

if __name__=='__main__':
    threading.Thread(target=candle_worker,daemon=True).start(); threading.Thread(target=quote_worker,daemon=True).start(); threading.Thread(target=analysis_worker,daemon=True).start()
    print('Gold Sentinel smart polling backend listening on',PORT,flush=True)
    ThreadingHTTPServer(('0.0.0.0',PORT),H).serve_forever()
