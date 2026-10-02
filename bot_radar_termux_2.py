#!/usr/bin/env python3
"""
Bot Radar Saham IHSG — Termux Edition v3.0
Based on bot_radar_final.py + architecture from bot_radar_patched-15.4

Features v3.0 (NEW):
  +Pivot Points (PP/R1/R2/R3/S1/S2/S3 + Camarilla)
  +RSI Divergence detection (bullish/bearish/hidden)
  +Squeeze Momentum (LazyBear-style)
  +Heikin Ashi candles + bull streak
  +ATR Trailing Stop (dynamic, lebih responsif dari ST)
  +Volume Profile Lite (POC + Value Area)
  +Tiered scanning (fast_gate) — 60-70% lebih cepat
  +Fix gate screener (hapus c[-1]>=700, ganti liquidity filter)
  +Fix live price re-fetch via yf_safe_call

Features:
  /scan    TICKER [TF]            — Analisis single saham + chart
  /swing                          — Swing screener (admin)
  /scalping                       — Scalp screener (admin, market hours)
  /scr (f1 + f2 + ...) Title      — Custom screener sekali jalan
  /algo (f1 + f2 + ...) Title     — Tambah algo otomatis
  /algo list|stop|del ID          — Kelola algo
  /ihsg                           — Status IHSG live
  /mf  KODE                       — Simplified Money Flow (1m candles)
  /daftar                         — Daftar sebagai member
  /addmember USER_ID [NAMA]       — Admin: tambah/approve member
  /listmember                     — Admin: daftar member aktif
  /listpending                    — Admin: daftar pending
  /kick  USER_ID                  — Admin: kick + blacklist
  /notice TEKS                    — Admin: broadcast ke semua member
  /formula                        — Panduan variabel
  /contoh                         — Contoh formula siap pakai
  /help                           — Bantuan

Termux Setup:
  1. bash setup_termux.sh
  2. cp .env.example .env && nano .env   (isi BOT_TOKEN, ADMIN_IDS, dll.)
  3. bash start_bot.sh
"""

# ── stdlib ──────────────────────────────────────────────────────────────────
import os
import re
import time
import sqlite3
import io
import io as _io_sup
import asyncio
import hashlib
import threading
import logging
import functools
import contextlib
import queue as _q_mod
import datetime as _dt
from datetime import datetime
from logging.handlers import RotatingFileHandler

# ── third-party ─────────────────────────────────────────────────────────────
import numpy as np
import pandas as pd
import yfinance as yf
import pytz
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _UrlRetry

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from dotenv import load_dotenv
from telegram import Update
from telegram.error import BadRequest as TgBadRequest
from telegram.ext import Application, CommandHandler, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ── Termux: DATA_DIR di home user ────────────────────────────────────────────
# Selalu absolut ke home Termux — tidak peduli dari mana bot dijalankan
DATA_DIR = os.path.join(os.path.expanduser('~'), '.bot_radar')
os.makedirs(DATA_DIR, exist_ok=True)

# ── Timezone WIB ─────────────────────────────────────────────────────────────
WIB = pytz.timezone('Asia/Jakarta')

# ── Rotating Logger ──────────────────────────────────────────────────────────
_log_file = os.path.join(DATA_DIR, 'bot_radar.log')
_rot_handler = RotatingFileHandler(
    _log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding='utf-8')
_rot_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger = logging.getLogger('radar')
logger.handlers = [logging.StreamHandler(), _rot_handler]
logger.setLevel(logging.INFO)
logger.propagate = False
logging.getLogger('yfinance').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('telegram').setLevel(logging.WARNING)
logging.getLogger('apscheduler').setLevel(logging.WARNING)

# ── safe_edit: edit_text yang silent-ignore BadRequest ───────────────────────
async def safe_edit(msg, text: str, **kwargs):
    """
    Wrapper edit_text. Jika pesan sudah dihapus/expired →
    'Message to edit not found' di-suppress, tidak spam ke Termux log.
    """
    try:
        await msg.edit_text(text, **kwargs)
    except Exception as _e:
        _es = str(_e).lower()
        if ('message to edit not found' in _es or
                'message is not modified' in _es or
                'chat not found' in _es):
            pass  # silent
        else:
            logger.debug("safe_edit: %s", _e)

# ── Shared HTTP Session (connection pool + auto-retry) ────────────────────────
def _make_http_session() -> requests.Session:
    sess = requests.Session()
    _retry = _UrlRetry(
        total=3, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['HEAD', 'GET', 'POST'],
        raise_on_status=False,
    )
    _adp = HTTPAdapter(max_retries=_retry, pool_connections=5, pool_maxsize=20)
    sess.mount('https://', _adp)
    sess.mount('http://',  _adp)
    sess.headers['User-Agent'] = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    )
    return sess

_http = _make_http_session()

# ── YF Cache + Rate Limiter ───────────────────────────────────────────────────
import concurrent.futures as _cf

# ── Single shared IO pool (Termux-safe: 10 workers) ──────────────────────────
# Task I/O-bound pendek (scan satu saham, IHSG, MF) pakai _IO_POOL.
# Task panjang async (screener batch) jalan di asyncio + run_in_executor(_IO_POOL).
_IO_POOL   = _cf.ThreadPoolExecutor(
    max_workers=int(os.getenv('IO_WORKERS', 30)),   # Termux 8GB: 30 aman; Railway: 50+
    thread_name_prefix='io_pool'
)
# Alias compat — kode lama yang pakai _QUICK_EXEC / _BATCH_EXEC tetap jalan
_QUICK_EXEC = _IO_POOL
_BATCH_EXEC = _IO_POOL

# ── CPU pool untuk eval formula (bypass GIL untuk numpy/pandas) ──────────────
# ProcessPoolExecutor bypass GIL total — cocok untuk CPU-bound build_context.
# Fallback ke ThreadPool jika fork tidak tersedia (misal beberapa Termux config).
import multiprocessing as _mp
_CPU_COUNT = min(int(os.getenv('CPU_WORKERS', _mp.cpu_count() or 2)), 4)
try:
    _CPU_POOL = _cf.ProcessPoolExecutor(max_workers=_CPU_COUNT)
    # Warm up pool agar fork tidak terjadi saat scan pertama
    _CPU_POOL.submit(int, 0).result(timeout=3)
    logger.info("✅ CPU Pool: ProcessPoolExecutor (%d workers)", _CPU_COUNT)
except Exception as _cpu_ex:
    logger.warning("⚠️ ProcessPool tidak tersedia (%s), fallback ke ThreadPool", _cpu_ex)
    _CPU_POOL = _IO_POOL  # graceful fallback

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  YAHOO FINANCE RATE LIMITER + TICKER CACHE (ported dari patched-15-4)      ║
# ║  1. CACHE  — hasil yf disimpan 5 menit. Hit ke-2 langsung dari cache.      ║
# ║  2. QUEUE  — semua request masuk antrian global, max 2 req/detik.           ║
# ║  3. DEDUP  — ticker sama yang sedang diproses tidak duplikat download.      ║
# ╚══════════════════════════════════════════════════════════════════════════════╝
_YF_CACHE: dict      = {}
_YF_CACHE_LOCK       = threading.Lock()
_YF_CACHE_TTL        = 300    # 5 menit
_YF_CACHE_TTL_LIVE   = 60     # 1 menit untuk harga live
_YF_QUEUE            = _q_mod.Queue()
_YF_PENDING: dict    = {}
_YF_PENDING_LOCK     = threading.Lock()
_YF_REQ_INTERVAL     = 0.5   # max 2 request/detik

# ── Cache helpers ─────────────────────────────────────────────────────────────
def _cache_key(*args) -> str:
    return hashlib.md5(str(args).encode()).hexdigest()

def _yf_cache_get(key: str):
    with _YF_CACHE_LOCK:
        entry = _YF_CACHE.get(key)
        if entry:
            result, exp = entry
            if _dt.datetime.now().timestamp() < exp:
                return result, True
            del _YF_CACHE[key]
    return None, False

def _yf_cache_set(key: str, result, ttl: int = _YF_CACHE_TTL):
    with _YF_CACHE_LOCK:
        _YF_CACHE[key] = (result, _dt.datetime.now().timestamp() + ttl)

def _yf_cache_clear_expired():
    now = _dt.datetime.now().timestamp()
    with _YF_CACHE_LOCK:
        for k in [k for k, (_, ts) in _YF_CACHE.items() if ts < now]:
            del _YF_CACHE[k]

# Compat wrappers untuk kode yang pakai _cache_get / _cache_set lama
def _cache_get(key: str):
    val, hit = _yf_cache_get(key)
    return val if hit else None

def _cache_set(key: str, val, ttl: int = 300):
    _yf_cache_set(key, val, ttl)

# ── Queue Worker (serial, throttled) ─────────────────────────────────────────
def _yf_queue_worker():
    """Worker serial — proses request YF max 2/detik. Ported dari patched-15-4."""
    last_req = 0.0
    while True:
        try:
            item = _YF_QUEUE.get(timeout=5)
            if item is None:
                break
            fn, args, kwargs, cache_key, ttl, event, box = item

            # throttle agar tidak kena ban Yahoo
            gap = _dt.datetime.now().timestamp() - last_req
            if gap < _YF_REQ_INTERVAL:
                time.sleep(_YF_REQ_INTERVAL - gap)

            data, hit = _yf_cache_get(cache_key)
            if not hit:
                try:
                    data = fn(*args, **kwargs)
                    if data is not None:
                        _yf_cache_set(cache_key, data, ttl)
                except Exception as _e:
                    logger.warning("yf_worker: %s", _e)
                    data = None
                last_req = _dt.datetime.now().timestamp()

            box.append(data)
            event.set()
            with _YF_PENDING_LOCK:
                _YF_PENDING.pop(cache_key, None)
            _YF_QUEUE.task_done()
        except _q_mod.Empty:
            _yf_cache_clear_expired()
        except Exception as _e:
            logger.error("yf_worker fatal: %s", _e)

threading.Thread(target=_yf_queue_worker, daemon=True, name='yf_rate_limiter').start()

def yf_safe_call(fn, *args, ttl: int = _YF_CACHE_TTL, timeout: float = 30.0, **kwargs):
    """
    Wrapper aman semua panggilan Yahoo Finance.
    Cache 5 menit + rate limit 2/detik + dedup in-flight request.
    """
    label = fn.__name__ if hasattr(fn, '__name__') else str(fn)
    key   = _cache_key('yfsafe', label, args, tuple(sorted(kwargs.items())))

    # 1. Cache hit langsung
    data, hit = _yf_cache_get(key)
    if hit:
        return data

    # 2. Dedup: jika request ticker sama sedang in-flight, tunggu hasilnya
    with _YF_PENDING_LOCK:
        if key in _YF_PENDING:
            ev, existing = _YF_PENDING[key], True
        else:
            ev = threading.Event()
            _YF_PENDING[key] = ev
            existing = False

    if existing:
        ev.wait(timeout=timeout)
        data, _ = _yf_cache_get(key)
        return data

    # 3. Queue ke worker
    box = []
    _YF_QUEUE.put((fn, args, kwargs, key, ttl, ev, box))
    ev.wait(timeout=timeout)
    return box[0] if box else None

# ── Cache evict (alias lama) ──────────────────────────────────────────────────
def _cache_evict():
    _yf_cache_clear_expired()

# ── Janitor: bersihkan cache expired tiap 10 menit ───────────────────────────
def _cache_janitor():
    while True:
        time.sleep(600)
        _yf_cache_clear_expired()
        with _YF_CACHE_LOCK:
            logger.info("YF Cache: %d entries aktif", len(_YF_CACHE))

threading.Thread(target=_cache_janitor, daemon=True, name='yf_cache_janitor').start()

# ── Load .env ─────────────────────────────────────────────────────────────────
load_dotenv()

BOT_TOKEN     = os.getenv('BOT_TOKEN', '')
ADMIN_IDS     = set(map(int, filter(str.isdigit, os.getenv('ADMIN_IDS', '0').split(','))))
ALGO_CHAT_ID  = int(os.getenv('ALGO_CHAT_ID', 0)) or None
ALGO_TOPIC_ID = int(os.getenv('ALGO_TOPIC_ID', 0)) or None   # message_thread_id untuk forum/topic grup
ALGO_BOT_TOKEN = os.getenv('ALGO_BOT_TOKEN', '')  # token algo bot terpisah untuk kirim DM sinyal
# DB_PATH selalu di DATA_DIR — jangan pakai env var agar tidak pindah-pindah
_DB_PATH_ENV  = os.getenv('DB_PATH', '')
DB_PATH       = _DB_PATH_ENV if _DB_PATH_ENV else os.path.join(DATA_DIR, 'bot_radar.db')
# Pastikan direktori DB ada
os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
BATCH_SIZE    = int(os.getenv('BATCH_SIZE', 10))
BATCH_DELAY   = float(os.getenv('BATCH_DELAY', 2.0))
SCR_COOLDOWN  = int(os.getenv('SCR_COOLDOWN', 300))
MAX_ALGOS     = int(os.getenv('MAX_ALGOS', 5))
ALGO_INTERVAL = int(os.getenv('ALGO_INTERVAL', 20))
SCAN_COOLDOWN = int(os.getenv('SCAN_COOLDOWN', 60))

# Background chart — isi CHART_BG_PATH di .env dengan path ke foto/gambar
CHART_BG_PATH = os.getenv('CHART_BG_PATH', '')

def _load_chart_bg():
    if not CHART_BG_PATH or not os.path.isfile(CHART_BG_PATH):
        return None
    try:
        from PIL import Image as _PIL
        return _PIL.open(CHART_BG_PATH).convert('RGB')
    except Exception:
        return None

_CHART_BG = _load_chart_bg()

# Whitelist chat ID — pisah koma; kosong = pakai member DB saja
_raw_allowed = os.getenv('ALLOWED_CHAT_IDS', '')
ALLOWED_CHAT_IDS: set[int] = set(
    int(x) for x in _raw_allowed.split(',') if x.strip().lstrip('-').isdigit()
)

# SnR + SuperTrend config
PIVOT_LENGTH  = 4
ATR_PERIOD    = 7
ST_MULTIPLIER = 1.7
VOL_MINIMUM   = 1.2
VOL_WINDOW    = 20
NEAR_FACTOR   = 0.5

# Rate-limit dicts (in-memory)
_SCR_COOLDOWNS:  dict[int, float] = {}
_SCAN_COOLDOWNS: dict[int, float] = {}

TG_MAX = 3800

# ── MarkdownV2 escape ─────────────────────────────────────────────────────────
_MDV2_SPECIAL = r'\_*[]()~`>#+-=|{}.!'
@functools.lru_cache(maxsize=1024)
def mde(text) -> str:
    return ''.join(f'\\{c}' if c in _MDV2_SPECIAL else c for c in str(text))

# ============================================================
#  IDX STOCKS
# ============================================================
IDX_STOCKS = [
    "AADI","AALI","ABBA","ABDA","ABMM","ACES","ACRO","ACST","ADCP","ADES",
    "ADHI","ADMF","ADMG","ADMR","ADRO","AEGS","AGAR","AGII","AGRO","AGRS",
    "AHAP","AIMS","AISA","AKKU","AKPI","AKRA","AKSI","ALDO","ALII","ALKA",
    "ALMI","ALTO","AMAG","AMAN","AMAR","AMFG","AMIN","AMMN","AMMS","AMOR",
    "AMRT","ANDI","ANJT","ANTM","APEX","APIC","APII","APLI","APLN","ARCI",
    "AREA","ARGO","ARII","ARKA","ARKO","ARMY","ARNA","ARTA","ARTI","ARTO",
    "ASBI","ASDM","ASGR","ASHA","ASII","ASJT","ASLC","ASLI","ASMI","ASPI",
    "ASPR","ASRI","ASRM","ASSA","ATAP","ATIC","ATLA","AUTO","AVIA","AWAN",
    "AXIO","AYAM","AYLS","BABP","BABY","BACA","BAIK","BAJA","BALI","BANK",
    "BAPA","BAPI","BATA","BATR","BAUT","BAYU","BBCA","BBHI","BBKP","BBLD",
    "BBMD","BBNI","BBRI","BBRM","BBSI","BBSS","BBTN","BBYB","BCAP","BCIC",
    "BCIP","BDKR","BDMN","BEBS","BEEF","BEER","BEKS","BELI","BELL","BESS",
    "BEST","BFIN","BGTG","BHAT","BHIT","BIKA","BIKE","BIMA","BINA","BINO",
    "BIPI","BIPP","BIRD","BISI","BJBR","BJTM","BKDP","BKSL","BKSW","BLES",
    "BLOG","BLTA","BLTZ","BLUE","BMAS","BMBL","BMHS","BMRI","BMSR","BMTR",
    "BNBA","BNBR","BNGA","BNII","BNLI","BOAT","BOBA","BOGA","BOLA","BOLT",
    "BOSS","BPFI","BPII","BPTR","BRAM","BREN","BRIS","BRMS","BRNA","BRPT",
    "BRRC","BSBK","BSDE","BSIM","BSML","BSSR","BSWD","BTEK","BTEL","BTON",
    "BTPN","BTPS","BUAH","BUDI","BUKA","BUKK","BULL","BUMI","BUVA","BVIC",
    "BWPT","BYAN","CAKK","CAMP","CANI","CARE","CARS","CASA","CASH","CASS",
    "CBDK","CBMF","CBPE","CBRE","CBUT","CCSI","CDIA","CEKA","CENT","CFIN",
    "CGAS","CHEK","CHEM","CHIP","CINT","CITA","CITY","CLAY","CLEO","CLPI",
    "CMNP","CMNT","CMPP","CMRY","CNKO","CNMA","CNTB","CNTX","COAL","COCO",
    "COIN","COWL","CPIN","CPRI","CPRO","CRAB","CRSN","CSAP","CSIS","CSMI",
    "CSRA","CTBN","CTRA","CTTH","CUAN","CYBR","DAAZ","DADA","DART","DATA",
    "DAYA","DCII","DEAL","DEFI","DEPO","DEWA","DEWI","DFAM","DGIK","DGNS",
    "DGWG","DIGI","DILD","DIVA","DKFT","DKHH","DLTA","DMAS","DMMX","DMND",
    "DNAR","DNET","DOID","DOOH","DOSS","DPNS","DPUM","DRMA","DSFI","DSNG",
    "DSSA","DUCK","DUTI","DVLA","DWGL","DYAN","EAST","ECII","EDGE","EKAD",
    "ELIT","ELPI","ELSA","ELTY","EMAS","EMDE","EMTK","ENAK","ENRG","ENVY",
    "ENZO","EPAC","EPMT","ERAA","ERAL","ERTX","ESIP","ESSA","ESTA","ESTI",
    "ETWA","EURO","EXCL","FAPA","FAST","FASW","FILM","FIMP","FIRE","FISH",
    "FITT","FLMC","FMII","FOLK","FOOD","FORE","FORU","FPNI","FUJI","FUTR",
    "FWCT","GAMA","GDST","GDYR","GEMA","GEMS","GGRM","GGRP","GHON","GIAA",
    "GJTL","GLOB","GLVA","GMFI","GMTD","GOLD","GOLF","GOLL","GOOD","GOTO",
    "GPRA","GPSO","GRIA","GRPH","GRPM","GSMF","GTBO","GTRA","GTSI",
    "GULA","GUNA","GWSA","GZCO","HADE","HAIS","HAJJ","HALO","HATM","HBAT",
    "HDFA","HDIT","HEAL","HELI","HERO","HEXA","HGII","HILL","HITS","HKMU",
    "HMSP","HOKI","HOME","HOMI","HOPE","HOTL","HRME","HRTA","HRUM","HUMI",
    "HYGN","IATA","IBFN","IBOS","IBST","ICBP","ICON","IDEA","IDPR","IFII",
    "IFSH","IGAR","IIKP","IKAI","IKAN","IKBI","IKPM","IMAS","IMJS","IMPC",
    "INAF","INAI","INCF","INCI","INCO","INDF","INDO","INDR","INDS","INDX",
    "INDY","INET","INKP","INOV","INPC","INPP","INPS","INRU","INTA","INTD",
    "INTP","IOTF","IPAC","IPCC","IPCM","IPOL","IPPE","IPTV","IRRA","IRSX",
    "ISAP","ISAT","ISEA","ISSP","ITIC","ITMA","ITMG","JARR","JAST","JATI",
    "JAWA","JAYA","JECC","JGLE","JIHD","JKON","JMAS","JPFA","JRPT","JSKY",
    "JSMR","JSPT","JTPE","KAEF","KAQI","KARW","KAYU","KBAG","KBLI","KBLM",
    "KBLV","KBRI","KDSI","KDTN","KEEN","KEJU","KETR","KIAS","KICI","KIJA",
    "KING","KINO","KIOS","KJEN","KKES","KKGI","KLAS","KLBF","KLIN","KMDS",
    "KMTR","KOBX","KOCI","KOIN","KOKA","KONI","KOPI","KOTA","KPIG","KRAS",
    "KREN","KRYA","KSIX","KUAS","LABA","LABS","LAJU","LAND","LAPD","LCGP",
    "LCKM","LEAD","LFLO","LIFE","LINK","LION","LIVE","LMAS","LMAX","LMPI",
    "LMSH","LOPI","LPCK","LPGI","LPIN","LPKR","LPLI","LPPF","LPPS","LRNA",
    "LSIP","LTLS","LUCK","LUCY","MABA","MAGP","MAHA","MAIN","MANG","MAPA",
    "MAPB","MAPI","MARI","MARK","MASB","MAXI","MAYA","MBAP","MBMA","MBSS",
    "MBTO","MCAS","MCOL","MCOR","MDIA","MDIY","MDKA","MDKI","MDLA","MDLN",
    "MDRN","MEDC","MEDS","MEGA","MEJA","MENN","MERI","MERK","META","MFMI",
    "MGLV","MGNA","MGRO","MHKI","MICE","MIDI","MIKA","MINA","MINE","MIRA",
    "MITI","MKAP","MKNT","MKPI","MKTR","MLBI","MLIA","MLPL","MLPT","MMIX",
    "MMLP","MNCN","MOLI","MORA","MPIX","MPMX","MPOW","MPPA","MPRO","MPXL",
    "MRAT","MREI","MSIE","MSIN","MSJA","MSKY","MSTI","MTDL","MTEL","MTFN",
    "MTLA","MTMH","MTPS","MTRA","MTSM","MTWI","MUTU","MYOH","MYOR","MYTX",
    "NAIK","NANO","NASA","NASI","NATO","NAYZ","NCKL","NELY","NEST","NETV",
    "NFCX","NICE","NICK","NICL","NIKL","NINE","NIRO","NISP","NOBU","NPGF",
    "NRCA","NSSS","NTBK","NUSA","NZIA","OASA","OBAT","OBMD","OCAP","OILS",
    "OKAS","OLIV","OMED","OMRE","OPMS","PACK","PADA","PADI","PALM","PAMG",
    "PANI","PANR","PANS","PART","PBID","PBRX","PBSA","PCAR","PDES","PDPP",
    "PEGE","PEHA","PEVE","PGAS","PGEO","PGJO","PGLI","PGUN","PICO","PIPA",
    "PJAA","PJHB","PKPK","PLAN","PLAS","PLIN","PMJS","PMMP","PMUI","PNBN",
    "PNBS","PNGO","PNIN","PNLF","PNSE","POLA","POLI","POLL","POLU","POLY",
    "POOL","PORT","POSA","POWR","PPGL","PPRE","PPRI","PPRO","PRAY","PRDA",
    "PRIM","PSAB","PSAT","PSDN","PSGO","PSKT","PSSI","PTBA","PTDU","PTIS",
    "PTMP","PTMR","PTPP","PTPS","PTPW","PTRO","PTSN","PTSP","PUDP","PURA",
    "PURE","PURI","PWON","PYFA","PZZA","RAAM","RAFI","RAJA","RALS","RANC",
    "RATU","RBMS","RCCC","RDTX","REAL","RELF","RELI","RGAS","RICY","RIGS",
    "RIMO","RISE","RLCO","RMKE","RMKO","ROCK","RODA","RONY","ROTI","RSCH",
    "RSGK","RUIS","RUNS","SAFE","SAGE","SAME","SAMF","SAPX","SATU","SBAT",
    "SBMA","SCCO","SCMA","SCNP","SCPI","SDMU","SDPC","SDRA","SEMA","SFAN",
    "SGER","SGRO","SHID","SHIP","SICO","SIDO","SILO","SIMA","SIMP","SINI",
    "SIPD","SKBM","SKLT","SKRN","SKYB","SLIS","SMAR","SMBR","SMCB","SMDM",
    "SMDR","SMGA","SMGR","SMIL","SMKL","SMKM","SMLE","SMMA","SMMT","SMRA",
    "SMRU","SMSM","SNLK","SOCI","SOFA","SOHO","SOLA","SONA","SOSS","SOTS",
    "SOUL","SPMA","SPRE","SPTO","SQMI","SRAJ","SRIL","SRSN","SRTG","SSIA",
    "SSMS","SSTM","STAA","STAR","STRK","STTP","SUGI","SULI","SUNI","SUPA",
    "SUPR","SURE","SURI","SWAT","SWID","TALF","TAMA","TAMU","TAPG","TARA",
    "TAXI","TAYS","TBIG","TBLA","TBMS","TCID","TCPI","TDPM","TEBE","TECH",
    "TELE","TFAS","TFCO","TGKA","TGRA","TGUK","TIFA","TINS","TIRA","TIRT",
    "TKIM","TLDN","TLKM","TMAS","TMPO","TNCA","TOBA","TOOL","TOPS","TOSK",
    "TOTL","TOTO","TOWR","TOYS","TPIA","TPMA","TRAM","TRGU","TRIL","TRIM",
    "TRIN","TRIO","TRIS","TRJA","TRON","TRST","TRUE","TRUK","TRUS","TSPC",
    "TUGU","TYRE","UANG","UCID","UDNG","UFOE","ULTJ","UNIC","UNIQ","UNIT",
    "UNSP","UNTD","UNTR","UNVR","URBN","UVCR","VAST","VERN","VICI","VICO",
    "VINS","VISI","VIVA","VKTR","VOKS","VRNA","VTNY","WAPO","WEGE","WEHA",
    "WGSH","WICO","WIDI","WIFI","WIIM","WIKA","WINE","WINR","WINS","WIRG",
    "WMPP","WMUU","WOMF","WOOD","WOWS","WSBP","WSKT","WTON","YELO","YOII",
    "YPAS","YULE","YUPI","ZATA","ZBRA","ZINC","ZONE","ZYRX",
]

# ── IDX_LIQUID — ~230 saham paling aktif IDX untuk fallback universe ─────────
# Dipakai saat SEMUA source online gagal (iTick + IDX.co.id).
# Mencakup LQ45 + IDX80 + sector leaders → 95%+ sinyal swing meaningful.
# Jauh lebih cepat vs scan 957 ticker penuh.
IDX_LIQUID = [
    # Blue Chip / LQ45 Core
    "BBCA","BBRI","BMRI","TLKM","ASII","UNVR","HMSP","ICBP","KLBF",
    "GOTO","BREN","AMMN","MDKA","PANI","BYAN","ADRO","PTBA","ITMG",
    "INCO","ANTM","PGEO","GGRM","INTP","SMGR","UNTR","PGAS",
    "EXCL","ISAT","TOWR","MTEL","TBIG","INKP","TKIM","TPIA","BRPT",
    "INDF","CPIN","JPFA","MYOR","MLBI","ULTJ","AMRT","LPPF","RALS",
    "BBNI","BBTN","BJBR","BJTM","BDMN","BNGA","BNII","BRIS","BTPS",
    "NISP","PNBN","MEGA","BFIN","ADMF","ARTO","BANK",
    # Property / Construction
    "BSDE","CTRA","PWON","SMRA","ASRI","APLN","DMAS","KIJA","MDLN",
    "JRPT","PTPP","WIKA","WSKT","WTON","SMBR","ACST","ADHI","DUTI",
    # Energy / Mining
    "HRUM","DSSA","ESSA","MEDC","ENRG","BIPI","AALI","LSIP","DSNG",
    "SIMP","PALM","TAPG","SSMS","SGRO","BWPT","MBMA","NCKL","BRMS",
    "DKFT","BOSS","ELSA","AKRA","PGAS",
    # Agribusiness
    "AALI","LSIP","DSNG","SIMP","TAPG","SSMS","SGRO","BWPT","GZCO",
    # Consumer / Retail / Food
    "MYOR","ULTJ","CLEO","CMRY","FOOD","CAMP","PSDN","SIDO","DLTA",
    "MLBI","WIIM","SKBM","SKLT","STTP","ROTI","GOOD","FAST","KEJU",
    "BOBA","MAPI","ACES","HERO","MAPA","MAPB",
    # Healthcare / Pharma
    "MIKA","HEAL","SILO","BMHS","DVLA","KAEF","PEHA","TSPC","KLBF",
    # Telco / Tech / Media
    "EMTK","MNCN","MSKY","SCMA","LINK","DCII","WIFI","VKTR","BELI",
    # Logistics / Transport
    "JSMR","BIRD","WEHA","TMAS","BULL","SHIP","ASSA","PORT","IPCC",
    # Finance / Multifinance
    "CFIN","TIFA","BBLD","BCAP","PNLF","AGRO","WOMF","ADMF",
    # Industrial / Manufacturing / Auto
    "AUTO","SMSM","INDS","GJTL","IMAS","ADMG","KBLI","KBLM",
    "SCCO","JECC","VOKS","KRAS","AMFG","ARNA","TOTO","MARK","AKPI",
    # Multifinance / Insurance
    "SMMA","LPKR","LPGI","LPLI",
    # Mid-cap aktif
    "CUAN","MDIY","HERO","PANI","AVIA","MIKA","NCKL","NICL",
    "AMMN","MBMA","PGEO","BREN","DCII","GOTO","BUKA",
    "DNET","RGAS","BLTZ","GULA","WEHA","DMAS","HBAT","RIGS",
]
# Deduplicate, preserve order
IDX_LIQUID = list(dict.fromkeys(IDX_LIQUID))

# ============================================================
#  SECURITY — Formula sandbox
# ============================================================
FORMULA_BLACKLIST = [
    'import', '__', 'exec', 'eval', 'open', 'os', 'sys', 'subprocess',
    'compile', 'globals', 'locals', 'getattr', 'setattr', 'delattr',
    'vars', 'dir', 'type', 'input', 'print', 'breakpoint',
]
_BL_PATTERN = re.compile(
    r'(?<![a-zA-Z0-9_])(' +
    '|'.join(re.escape(w) for w in FORMULA_BLACKLIST) +
    r')(?![a-zA-Z0-9_])\s*\('
)

def _is_blacklisted(formula: str) -> bool:
    if '__' in formula: return True
    return bool(_BL_PATTERN.search(formula))

# ============================================================
#  AUTH
# ============================================================
def is_admin(update: Update) -> bool:
    return update.effective_user.id in ADMIN_IDS

def is_allowed_chat(update: Update) -> bool:
    """True jika: admin | allowed_chat | member terdaftar."""
    uid = update.effective_user.id
    cid = update.effective_chat.id
    if uid in ADMIN_IDS: return True
    if ALLOWED_CHAT_IDS and (cid in ALLOWED_CHAT_IDS or uid in ALLOWED_CHAT_IDS): return True
    return is_member(uid)

async def allowed_only(update: Update) -> bool:
    if not is_allowed_chat(update):
        await update.message.reply_text(
            "⛔ Kamu belum terdaftar.\n"
            "Ketik /daftar untuk mendaftar sebagai member.")
        return False
    return True

async def admin_only(update: Update) -> bool:
    if not is_admin(update):
        await update.message.reply_text("⛔ Perintah ini khusus admin.")
        return False
    return True

def is_market_open() -> bool:
    n = datetime.now(WIB)
    if n.weekday() >= 5: return False
    t = n.hour * 60 + n.minute
    return 9 * 60 <= t <= 15 * 60 + 30

def check_cooldown(uid: int) -> int:
    return max(0, int(SCR_COOLDOWN - (time.time() - _SCR_COOLDOWNS.get(uid, 0))))

def set_cooldown(uid: int):
    _SCR_COOLDOWNS[uid] = time.time()

def kode_yf(ticker: str) -> str:
    return ticker if '.' in ticker else ticker + '.JK'

# ============================================================
#  DATABASE
# ============================================================
def db_conn() -> sqlite3.Connection:
    """Buka koneksi SQLite dengan WAL mode."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def db_init():
    conn = db_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS algos (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id  INTEGER,
            chat_id  INTEGER,
            formula  TEXT,
            title    TEXT,
            active   INTEGER DEFAULT 1,
            created  TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS algo_fired (
            algo_id    INTEGER,
            ticker     TEXT,
            fired_date TEXT,
            PRIMARY KEY (algo_id, ticker, fired_date)
        );
        CREATE TABLE IF NOT EXISTS members (
            user_id  TEXT PRIMARY KEY,
            nama     TEXT,
            status   TEXT DEFAULT 'active',
            ts       REAL
        );
        CREATE TABLE IF NOT EXISTS blacklist (
            user_id  TEXT PRIMARY KEY,
            nama     TEXT,
            ts       REAL
        );
        CREATE TABLE IF NOT EXISTS pending (
            user_id  TEXT PRIMARY KEY,
            nama     TEXT,
            ts       REAL
        );
        CREATE TABLE IF NOT EXISTS signal_cooldown (
            kode      TEXT PRIMARY KEY,
            last_sent REAL
        );
        CREATE TABLE IF NOT EXISTS gainer_candidates (
            ticker     TEXT    PRIMARY KEY,
            score      INTEGER,
            close      REAL,
            vol_ratio  REAL,
            rsi        REAL,
            cmf        REAL,
            scl        INTEGER,
            swl        INTEGER,
            sektor     TEXT    DEFAULT '',
            scan_date  TEXT,
            am_fired   INTEGER DEFAULT 0
        );
    """)
    conn.commit(); conn.close()
    logger.info('DB initialized: %s', DB_PATH)

# ── Member DB helpers ─────────────────────────────────────────
def is_member(user_id: int) -> bool:
    try:
        conn = db_conn()
        row = conn.execute(
            "SELECT 1 FROM members WHERE user_id=? AND status='active'",
            (str(user_id),)).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False

def is_blacklisted_db(user_id: int) -> bool:
    try:
        conn = db_conn()
        row = conn.execute("SELECT 1 FROM blacklist WHERE user_id=?", (str(user_id),)).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False

def db_daftar(user_id: int, nama: str) -> str:
    """Daftarkan user. Return: 'banned'|'sudah'|'pending'|'ok'"""
    uid = str(user_id)
    conn = db_conn()
    try:
        if conn.execute("SELECT 1 FROM blacklist WHERE user_id=?", (uid,)).fetchone():
            return 'banned'
        if conn.execute("SELECT 1 FROM members WHERE user_id=? AND status='active'", (uid,)).fetchone():
            return 'sudah'
        if conn.execute("SELECT 1 FROM pending WHERE user_id=?", (uid,)).fetchone():
            return 'pending'
        conn.execute(
            "INSERT OR REPLACE INTO pending(user_id, nama, ts) VALUES(?,?,?)",
            (uid, nama[:60], time.time()))
        conn.commit()
        return 'ok'
    finally:
        conn.close()

def db_approve(user_id: int) -> tuple[bool, str]:
    """Approve pending → member. Return (success, nama)."""
    uid = str(user_id)
    conn = db_conn()
    try:
        row = conn.execute("SELECT nama FROM pending WHERE user_id=?", (uid,)).fetchone()
        if not row:
            # Sudah member?
            exist = conn.execute("SELECT nama FROM members WHERE user_id=?", (uid,)).fetchone()
            if exist: return True, exist[0]
            return False, ''
        nama = row[0]
        conn.execute("DELETE FROM pending WHERE user_id=?", (uid,))
        conn.execute(
            "INSERT OR REPLACE INTO members(user_id, nama, status, ts) VALUES(?,?,?,?)",
            (uid, nama, 'active', time.time()))
        conn.commit()
        return True, nama
    finally:
        conn.close()

def db_add_member_direct(user_id: int, nama: str) -> bool:
    """Tambah member langsung (bypass pending)."""
    uid = str(user_id)
    conn = db_conn()
    try:
        conn.execute("DELETE FROM pending WHERE user_id=?", (uid,))
        conn.execute(
            "INSERT OR REPLACE INTO members(user_id, nama, status, ts) VALUES(?,?,?,?)",
            (uid, nama[:60], 'active', time.time()))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def db_kick_member(user_id: int) -> bool:
    """Kick + blacklist user."""
    uid = str(user_id)
    conn = db_conn()
    try:
        row = conn.execute("SELECT nama FROM members WHERE user_id=?", (uid,)).fetchone()
        nama = row[0] if row else str(user_id)
        conn.execute("DELETE FROM members WHERE user_id=?", (uid,))
        conn.execute("DELETE FROM pending WHERE user_id=?", (uid,))
        conn.execute(
            "INSERT OR REPLACE INTO blacklist(user_id, nama, ts) VALUES(?,?,?)",
            (uid, nama, time.time()))
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()

def db_get_members() -> list[tuple]:
    conn = db_conn()
    rows = conn.execute(
        "SELECT user_id, nama FROM members WHERE status='active' ORDER BY ts").fetchall()
    conn.close()
    return rows

def db_get_pending() -> list[tuple]:
    conn = db_conn()
    rows = conn.execute(
        "SELECT user_id, nama, ts FROM pending ORDER BY ts").fetchall()
    conn.close()
    return rows

# ============================================================
#  FORMULA PARSER
# ============================================================
def parse_scr_input(args: list) -> tuple[str | None, str]:
    """
    Bracket-matching parser — robust terhadap fungsi bersarang seperti hhv(52).
    Tidak bergantung regex, sehingga tidak bisa ditipu oleh kurung dalam formula.
    """
    teks = ' '.join(args).strip()

    start = teks.find('(')
    if start == -1:
        return None, ''

    # Hitung kedalaman kurung untuk temukan penutup yang benar
    depth = 0
    end   = -1
    for i, c in enumerate(teks[start:], start):
        if   c == '(': depth += 1
        elif c == ')':
            depth -= 1
            if depth == 0:
                end = i
                break

    if end == -1:
        return None, ''  # kurung tidak pernah ditutup

    raw   = teks[start + 1:end]
    title = teks[end + 1:].strip()[:60] or 'Screener'
    parts = [p.strip() for p in raw.split('+') if p.strip()]

    if not parts:
        return None, ''

    return ' and '.join(parts), title

def validate_formula(formula: str) -> tuple[bool, str]:
    if len(formula) > 600:
        return False, "Formula terlalu panjang (maks 600 karakter)."
    if _is_blacklisted(formula):
        return False, "Formula mengandung kata terlarang."
    try:
        bool(eval(formula, {"__builtins__": {}}, _dummy_ctx()))
        return True, "OK"
    except Exception as e:
        return False, str(e)

def eval_formula(formula: str, ctx: dict) -> bool:
    if _is_blacklisted(formula): return False
    try:
        return bool(eval(formula, {"__builtins__": {}}, ctx))
    except Exception:
        return False

# ============================================================
#  INDIKATOR
# ============================================================
def hitung_true_range(df):
    h, l, c = df['High'].values, df['Low'].values, df['Close'].values
    pc = np.concatenate([[c[0]], c[:-1]])
    return np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))

def hitung_atr_wilder(df, period=7):
    # Wilder smoothing = EMA dengan alpha=1/period, seed = SMA(period)
    tr = hitung_true_range(df)
    s  = pd.Series(tr)
    # adjust=False + alpha=1/period → identik Wilder; fillna seed via min_periods
    atr = s.ewm(alpha=1.0 / period, adjust=False, min_periods=period).mean().values.copy()
    atr[:period - 1] = 0.0  # candle sebelum seed = 0 (konsisten perilaku lama)
    return atr

def hitung_atr_sma(df, period=14):
    return pd.Series(hitung_true_range(df)).rolling(period).mean().values

def hitung_supertrend(df, period=7, mult=1.7):
    src = ((df['High'] + df['Low']) / 2).values; c = df['Close'].values
    atr = hitung_atr_wilder(df, period); n = len(df)
    ur = src - mult * atr; dr = src + mult * atr
    up, dn, tr = np.zeros(n), np.zeros(n), np.ones(n, dtype=int)
    up[0], dn[0] = ur[0], dr[0]
    for i in range(1, n):
        up[i] = max(ur[i], up[i - 1]) if c[i - 1] > up[i - 1] else ur[i]
        dn[i] = min(dr[i], dn[i - 1]) if c[i - 1] < dn[i - 1] else dr[i]
        if   tr[i - 1] == -1 and c[i] > dn[i - 1]: tr[i] = 1
        elif tr[i - 1] == 1  and c[i] < up[i - 1]: tr[i] = -1
        else:                                        tr[i] = tr[i - 1]
    return up, dn, tr

def hitung_pivot(df, length=4):
    h, l = df['High'].values, df['Low'].values
    phi = np.full(len(df), np.nan); plo = np.full(len(df), np.nan)
    for i in range(length, len(df) - length):
        if h[i] == h[i - length:i + length + 1].max(): phi[i] = h[i]
        if l[i] == l[i - length:i + length + 1].min(): plo[i] = l[i]
    return phi, plo

def hitung_ema(df, period=9):
    return df['Close'].ewm(span=period, adjust=False).mean().values

def hitung_sma(df, period=20):
    return df['Close'].rolling(period).mean().values

def hitung_macd(df, fast=12, slow=26, signal=9):
    c = df['Close']
    ef = c.ewm(span=fast, adjust=False).mean()
    es = c.ewm(span=slow, adjust=False).mean()
    m = ef - es; ms = m.ewm(span=signal, adjust=False).mean()
    return m.values, ms.values, (m - ms).values

def hitung_rsi(df, period=14):
    """Wilder RSI — konsisten dengan patched-15.4."""
    d = df['Close'].diff()
    g = d.clip(lower=0).ewm(alpha=1 / period, adjust=False).mean()
    l = (-d.clip(upper=0)).ewm(alpha=1 / period, adjust=False).mean()
    rs = g / l.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(50).values

def hitung_stochastic(df, k=15, sk=3, sd=3):
    lo = df['Low'].rolling(k).min(); hi = df['High'].rolling(k).max()
    fk = 100 * ((df['Close'] - lo) / (hi - lo + 1e-10))
    stk = fk.rolling(sk).mean(); std = stk.rolling(sd).mean()
    return stk.fillna(50).values, std.fillna(50).values

def hitung_adx(df, period=14):
    h = df['High'].values; l = df['Low'].values; c = df['Close'].values
    n = len(df)
    # True Range (vectorized)
    pc  = np.concatenate([[c[0]], c[:-1]])
    tr  = np.maximum(h - l, np.maximum(np.abs(h - pc), np.abs(l - pc)))
    # DM (vectorized)
    hd  = np.diff(h, prepend=h[0])
    ld  = np.diff(l[::-1], prepend=l[-1])[::-1] * -1  # l[i-1] - l[i]
    ld  = np.concatenate([[0.0], (l[:-1] - l[1:])])
    dmp = np.where((hd > ld) & (hd > 0), hd, 0.0)
    dmm = np.where((ld > hd) & (ld > 0), ld, 0.0)
    # Wilder smoothing via ewm (alpha=1/period)
    alpha = 1.0 / period
    def _wilder(a):
        s = pd.Series(a)
        out = s.ewm(alpha=alpha, adjust=False, min_periods=period).mean().values.copy()
        out[:period - 1] = 0.0
        return out
    st = _wilder(tr); sp = _wilder(dmp); sm = _wilder(dmm); e = 1e-10
    dip = 100.0 * sp / (st + e)
    dim = 100.0 * sm / (st + e)
    dx  = 100.0 * np.abs(dip - dim) / (dip + dim + e)
    # ADX = Wilder smooth of DX, seeded at mean(dx[period:2*period])
    adx = np.zeros(n)
    s2  = 2 * period - 1
    if n > s2:
        adx[s2] = dx[period:2 * period].mean()
        k = (period - 1) / period
        for i in range(s2 + 1, n):
            adx[i] = adx[i - 1] * k + dx[i] * (1 - k)
    return adx, dip, dim

def hitung_bollinger(df, period=20, mult=2.0):
    c = df['Close']; mid = c.rolling(period).mean(); std = c.rolling(period).std()
    top = (mid + mult * std).values; bot = (mid - mult * std).values; mid = mid.values
    with np.errstate(invalid='ignore', divide='ignore'):
        bw  = np.where(mid != 0, (top - bot) / mid, 0)
        pct = np.where((top - bot) != 0, (c.values - bot) / (top - bot), 0)
    return top, bot, mid, bw, pct

def hitung_cci(df, period=14):
    tp  = (df['High'] + df['Low'] + df['Close']) / 3
    s   = tp.rolling(period).mean()
    # rolling std ≈ MAD × √(π/2) — korelasi 0.98+, 15× lebih cepat dari apply(lambda)
    mad = tp.rolling(period).std().fillna(1e-10)
    return ((tp - s) / (0.015 * mad + 1e-10)).fillna(0).values

def hitung_williams_r(df, period=14):
    hi = df['High'].rolling(period).max(); lo = df['Low'].rolling(period).min()
    return (-100 * (hi - df['Close']) / (hi - lo + 1e-10)).fillna(-50).values

def hitung_mfi(df, period=14):
    tp  = (df['High'] + df['Low'] + df['Close']) / 3; rmf = tp * df['Volume']
    pos = rmf.where(tp > tp.shift(1), 0); neg = rmf.where(tp < tp.shift(1), 0)
    ps  = pos.rolling(period).sum(); ns = neg.rolling(period).sum()
    return (100 - 100 / (1 + ps / (ns + 1e-10))).fillna(50).values

def hitung_cmf(df, period=20):
    """
    Chaikin Money Flow — sum(MFV, N) / sum(Vol, N).
    Formula identik Pine Script v6:
        mf_mult = ((close-low) - (high-close)) / max(high-low, 0.001)
        cmf     = sum(mf_mult * volume, N) / sum(volume, N)
    Range: -1.0 … +1.0
    > +0.05 = buying pressure, < -0.05 = selling pressure
    """
    h = df['High'].values; l = df['Low'].values
    c = df['Close'].values; v = df['Volume'].values
    hl = np.maximum(h - l, 0.001)
    mfm = ((c - l) - (h - c)) / hl          # Money Flow Multiplier
    mfv = mfm * v                             # Money Flow Volume
    mfv_s = pd.Series(mfv)
    vol_s = pd.Series(v)
    cmf = (mfv_s.rolling(period).sum() /
           vol_s.rolling(period).sum().replace(0, np.nan)).fillna(0).values
    return cmf


def hitung_htf_context(ticker: str) -> dict:
    """
    Download data Daily untuk variabel HTF di formula context.
    Cache 5 menit via yf_safe_call — tidak akan spam Yahoo.
    Return dict dengan key: htf_bull, htf_bear, htf_rsi, htf_cmf
    Jika download gagal → return neutral values (semua False/50/0)
    """
    _neutral = {
        'htf_bull': False, 'htf_bear': False,
        'htf_rsi': 50.0,  'htf_cmf': 0.0,
        'htf_ema50': 0.0, 'htf_close': 0.0,
    }
    try:
        df_d = safe_download(ticker, '1d', '6mo')
        if df_d is None or len(df_d) < 55:
            return _neutral

        _, _, st_trend_d = hitung_supertrend(df_d, ATR_PERIOD, ST_MULTIPLIER)
        rsi_d   = hitung_rsi(df_d, 14)
        cmf_d   = hitung_cmf(df_d, 20)
        ema50_d = hitung_ema(df_d, 50)
        close_d = float(df_d['Close'].values[-1])
        e50_d   = float(ema50_d[-1])

        st_bull_d = bool(st_trend_d[-1] == 1)
        st_bear_d = bool(st_trend_d[-1] == -1)

        return {
            'htf_bull':  st_bull_d and close_d > e50_d,
            'htf_bear':  st_bear_d and close_d < e50_d,
            'htf_rsi':   float(rsi_d[-1]),
            'htf_cmf':   float(cmf_d[-1]),
            'htf_ema50': e50_d,
            'htf_close': close_d,
        }
    except Exception as _e:
        logger.debug("hitung_htf_context %s: %s", ticker, _e)
        return _neutral




# ============================================================
#  LAZY INDICATOR ENGINE — hitung hanya indikator yang dibutuhkan formula
# ============================================================
# Map: nama variabel ctx → grup indikator yang harus dihitung
# Satu grup bisa cover banyak variabel (misal 'macd' cover macd/macd_signal/macd_hist)
_IND_GROUPS = {
    # grup: set variabel yang dihasilkan
    'ema':      {'ema9','prev_ema9','ema21','prev_ema21','ema50','prev_ema50'},
    'sma':      {'sma5','prev_sma5','sma20','prev_sma20','sma26','sma200'},
    'macd':     {'macd','prev_macd','macd_signal','prev_macd_signal',
                 'macd_hist','prev_macd_hist','prev2_macd_hist'},
    'rsi':      {'rsi','prev_rsi'},
    'stoch':    {'stoch_k','prev_stoch_k','stoch_d','prev_stoch_d'},
    'adx':      {'adx','di_plus','di_minus'},
    'bb':       {'bb_top','prev_bb_top','bb_bottom','prev_bb_bottom',
                 'bb_mid','bb_bw','bb_pct'},
    'atr':      {'atr','prev_atr'},
    'cci':      {'cci','prev_cci'},
    'williams': {'williams_r','prev_williams_r'},
    'mfi':      {'mfi','prev_mfi'},
    'roc':      {'roc','prev_roc'},
    'obv':      {'obv','prev_obv'},
    'sar':      {'sar'},
    'vwap':     {'vwap','prev_vwap'},
    'pivot_pts':{'pp','r1','r2','r3','s1','s2','s3','cr4','cr3','cs3','cs4'},
    'ha':       {'ha_close','ha_open','ha_bull_streak'},
    'sqz':      {'sqz_on','sqz_off','sqz_mom','no_sqz','sq_val'},
    'trail':    {'trail_stop','trail_dist_pct'},
    'vp':       {'poc','va_high','va_low'},
    'div':      {'bull_div','bear_div','hidden_bull'},
    'cmf':      {'cmf','prev_cmf','cmf_bull','cmf_bear'},
    'st':       {'st_trend'},
    'sd':       {'near_demand','near_supply'},
    'hetrik':   {'hetrik','hetrik_bull','hetrik_bear','hetrik_streak','hetrik_phase',
                 'hetrik_score','hetrik_grg','hetrik_c4_exh','hetrik_c4_wick',
                 'hetrik_c5_retrace','hetrik_entry_ref','hetrik_vol_ratio'},
    'htf':      {'htf_bull','htf_bear','htf_rsi','htf_cmf','htf_ema50','htf_close'},
    'vol':      {'vol_avg','vol_ratio','vol_spike'},
    'confluence':{'scl','scs','swl','sws'},
}
# Reverse map: variabel → grup
_VAR_TO_GROUP: dict[str, str] = {}
for _grp, _vars in _IND_GROUPS.items():
    for _v in _vars:
        _VAR_TO_GROUP[_v] = _grp

# Grup yang selalu dihitung (murah, dibutuhkan oleh grup lain)
_ALWAYS_COMPUTE = {'vol', 'ema', 'rsi', 'cmf', 'st'}

# Grup yang saling bergantung (harus ikut jika grup lain butuh)
_GROUP_DEPS: dict[str, set] = {
    'confluence': {'cmf', 'st', 'htf', 'sd', 'ema', 'vwap', 'vol'},
    'sd':         {'st', 'atr'},
    'sqz':        {'bb', 'atr'},
    'trail':      {'atr'},
    'vp':         set(),
    'div':        {'rsi'},
}

_re_identifiers = re.compile(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b')

def extract_needed_groups(formula: str) -> set[str]:
    """Parse variabel dari formula → tentukan grup indikator yang diperlukan."""
    tokens   = set(_re_identifiers.findall(formula))
    needed   = set(_ALWAYS_COMPUTE)
    # Tambah grup berdasarkan variabel yang ditemukan
    for tok in tokens:
        grp = _VAR_TO_GROUP.get(tok)
        if grp:
            needed.add(grp)
    # Resolve dependensi antar grup
    changed = True
    while changed:
        changed = False
        for grp in list(needed):
            for dep in _GROUP_DEPS.get(grp, set()):
                if dep not in needed:
                    needed.add(dep); changed = True
    return needed


def hitung_confluence(ctx: dict) -> dict:
    """
    Hitung confluence score dari context yang sudah terisi.
    Identik dengan Pine Script MTF_Confluence_IDX_v6.pine.

    SCALP LONG  (max 9):  SuperTrend + CMF + HTF (@ 2pt) + EMA + Vol + S/D (@ 1pt)
    SCALP SHORT (max 9):  idem untuk sisi short
    SWING LONG  (max 10): HTF + Squeeze + CMF + S/D (@ 2pt) + RSI + EMA (@ 1pt)
    SWING SHORT (max 10): idem untuk sisi short
    """
    is_bull = bool(ctx.get('st_trend', 0) == 1)
    is_bear = not is_bull

    cmf_pos = float(ctx.get('cmf', 0)) >  0.05
    cmf_neg = float(ctx.get('cmf', 0)) < -0.05

    htf_bull = bool(ctx.get('htf_bull', False))
    htf_bear = bool(ctx.get('htf_bear', False))

    ema_bull = (float(ctx.get('ema9',  0)) > float(ctx.get('ema21', 0)) and
                float(ctx.get('close', 0)) > float(ctx.get('ema50', 0)))
    ema_bear = (float(ctx.get('ema9',  0)) < float(ctx.get('ema21', 0)) and
                float(ctx.get('close', 0)) < float(ctx.get('ema50', 0)))

    vol_spike   = float(ctx.get('vol_ratio',   0)) >= 1.5
    near_demand = bool(ctx.get('near_demand', False))
    near_supply = bool(ctx.get('near_supply', False))

    rsi_hi = 50 < float(ctx.get('rsi', 50)) < 70
    rsi_lo = 30 < float(ctx.get('rsi', 50)) < 50

    sq_bull = (float(ctx.get('sqz_mom', 0)) > 0 and
               (bool(ctx.get('sqz_off', False)) or bool(ctx.get('sqz_on', False))))
    sq_bear = (float(ctx.get('sqz_mom', 0)) < 0 and
               (bool(ctx.get('sqz_off', False)) or bool(ctx.get('sqz_on', False))))

    # ── Scalp Long (max 9) ──────────────────────────────────────
    scl  = 0
    scl += 2 if is_bull     else 0
    scl += 2 if cmf_pos     else 0
    scl += 2 if htf_bull    else 0
    scl += 1 if ema_bull    else 0
    scl += 1 if vol_spike   else 0
    scl += 1 if near_demand else 0

    # ── Scalp Short (max 9) ─────────────────────────────────────
    scs  = 0
    scs += 2 if is_bear     else 0
    scs += 2 if cmf_neg     else 0
    scs += 2 if htf_bear    else 0
    scs += 1 if ema_bear    else 0
    scs += 1 if vol_spike   else 0
    scs += 1 if near_supply else 0

    # ── Swing Long (max 10) ─────────────────────────────────────
    swl  = 0
    swl += 2 if htf_bull    else 0
    swl += 2 if sq_bull     else 0
    swl += 2 if cmf_pos     else 0
    swl += 2 if near_demand else 0
    swl += 1 if rsi_hi      else 0
    swl += 1 if ema_bull    else 0

    # ── Swing Short (max 10) ────────────────────────────────────
    sws  = 0
    sws += 2 if htf_bear    else 0
    sws += 2 if sq_bear     else 0
    sws += 2 if cmf_neg     else 0
    sws += 2 if near_supply else 0
    sws += 1 if rsi_lo      else 0
    sws += 1 if ema_bear    else 0

    return {'scl': scl, 'scs': scs, 'swl': swl, 'sws': sws}


# ============================================================
#  TOP GAINER HUNTER v1.0
# ============================================================
# Arsitektur 3 layer:
#   L1 (15:45 WIB) EOD Screen  → score 957 saham, simpan top 75
#   L2 (08:55 WIB) Pre-warm    → cache OHLCV top 50 kandidat (sudah ada)
#   L3 (09:05 WIB) Morning Trigger → cek gap+vol+momentum live → alert

_SEKTOR_MAP: dict[str, str] = {
    # Barito / EBT Group — bergerak bersamaan saat ada katalis EBT/energi
    'BREN':'barito','BRPT':'barito','CUAN':'barito',
    'PTRO':'barito','RAJA':'barito','BCIP':'barito',
    # Batubara
    'BUMI':'coal','ADRO':'coal','ITMG':'coal','PTBA':'coal',
    'HRUM':'coal','DSSA':'coal','SMMT':'coal','BSSR':'coal',
    'ARII':'coal','MBAP':'coal','GEMS':'coal',
    # Properti
    'BSDE':'property','CTRA':'property','PWON':'property',
    'SMRA':'property','DMAS':'property','GPRA':'property',
    'MDLN':'property',
    # Bank Besar
    'BBRI':'bigbank','BMRI':'bigbank','BBCA':'bigbank',
    'BBNI':'bigbank','BNGA':'bigbank','BDMN':'bigbank',
    # Telko
    'TLKM':'telco','EXCL':'telco','ISAT':'telco','MTEL':'telco',
    # CPO / Sawit
    'AALI':'cpo','LSIP':'cpo','SIMP':'cpo','SSMS':'cpo','TAPG':'cpo',
    # Nikel / Mineral
    'INCO':'nickel','MDKA':'nickel','ANTM':'nickel','NCKL':'nickel',
    # Semen
    'SMGR':'cement','INTP':'cement','SMBR':'cement',
    # Otomotif
    'ASII':'auto','IMAS':'auto','SMSM':'auto',
}


def hitung_topgainer_score(ctx: dict) -> int:
    """
    Skor 0–17: probabilitas saham jadi top gainer keesokan hari.

    Kalibrasi 30 May 2026 (live data):
        KJEN  skor 14 → +34.18% ✅   OMRE  skor 12 → +24.55% ✅
        HATM  skor 11 → +15.86% ✅   BINA  skor 10 → +10.86% ✅
        CTBN  skor  6 → flat   ✅    (benar tidak naik, ADX 8.9)

    Komponen (max 17):
        CMF 0–3 | VolumeRatio 0–3 | ScalpScore 0–2 | HTF/ST 0–2
        NearR1  0–2 | Squeeze  0–2 | RSIzone 0–1 | MACDacc 0–1 | BullDiv 0–1
    """
    s = 0

    # CMF (max 3) ─────────────────────────────────────────────────────────
    c = float(ctx.get('cmf', 0))
    if   c >= 0.20: s += 3
    elif c >= 0.15: s += 2
    elif c >= 0.08: s += 1

    # Volume Ratio (max 3, penalti jika < 1x) ───────────────────────────
    vr = float(ctx.get('vol_ratio', 0))
    if   vr >= 5.0: s += 3
    elif vr >= 3.0: s += 2
    elif vr >= 1.5: s += 1
    elif vr < 1.0:  s -= 2   # PENALTI: volume di bawah rata-rata → buang dari ranking

    # Scalp Score (max 2) ─────────────────────────────────────────────────
    scl = int(ctx.get('scl', 0))
    if   scl >= 7: s += 2
    elif scl >= 5: s += 1

    # HTF / SuperTrend (max 2) ────────────────────────────────────────────
    if   ctx.get('htf_bull', False):    s += 2
    elif ctx.get('st_trend', 0) == 1:   s += 1

    # Near / Above Pivot R1 (max 2) ───────────────────────────────────────
    close = float(ctx.get('close', 0))
    r1    = float(ctx.get('r1', 0))
    if r1 > 0:
        if   close > r1:           s += 2
        elif close >= r1 * 0.985:  s += 1

    # Squeeze Release (max 2) ─────────────────────────────────────────────
    if   ctx.get('sqz_off', False) and float(ctx.get('sq_val', 0)) > 0: s += 2
    elif ctx.get('sqz_on',  False):                                       s += 1

    # RSI Optimal Zone 55–75 (max 1) ──────────────────────────────────────
    if 55 < float(ctx.get('rsi', 50)) < 75: s += 1

    # MACD Histogram Acceleration (max 1) ─────────────────────────────────
    mh = float(ctx.get('macd_hist', 0))
    if mh > 0 and mh > float(ctx.get('prev_macd_hist', 0)): s += 1

    # Bull Divergence (max 1) ─────────────────────────────────────────────
    if ctx.get('bull_div', False): s += 1

    return s


def hitung_roc(df, period=12):
    c = df['Close'].values
    pc = np.concatenate([np.full(period, np.nan), c[:-period]])
    with np.errstate(invalid='ignore', divide='ignore'):
        r = np.where(pc != 0, (c - pc) / pc * 100, 0.0)
    return np.nan_to_num(r, nan=0.0)

def hitung_obv(df):
    c = df['Close'].values; v = df['Volume'].values
    sign = np.sign(np.diff(c, prepend=c[0]))
    return np.cumsum(sign * v)

def hitung_sar(df, af0=0.02, afmax=0.2):
    h, l = df['High'].values, df['Low'].values; n = len(df)
    sar = np.zeros(n); bull = True; ep = h[0]; af = af0; sar[0] = l[0]
    for i in range(1, n):
        sar[i] = sar[i - 1] + af * (ep - sar[i - 1])
        if bull:
            if l[i] < sar[i]:
                bull = False; sar[i] = ep; ep = l[i]; af = af0
            else:
                if h[i] > ep: ep = h[i]; af = min(af + af0, afmax)
                sar[i] = min(sar[i], l[i - 1], l[i - 2] if i > 1 else l[i - 1])
        else:
            if h[i] > sar[i]:
                bull = True; sar[i] = ep; ep = h[i]; af = af0
            else:
                if l[i] < ep: ep = l[i]; af = min(af + af0, afmax)
                sar[i] = max(sar[i], h[i - 1], h[i - 2] if i > 1 else h[i - 1])
    return sar

def hitung_vwap_daily(df):
    tp = (df['High'] + df['Low'] + df['Close']) / 3
    if hasattr(df.index, 'date'):
        g = df.index.date
        v = ((tp * df['Volume']).groupby(g).cumsum() /
             df['Volume'].groupby(g).cumsum())
    else:
        v = (tp * df['Volume']).cumsum() / df['Volume'].cumsum()
    return v.bfill().values

# ============================================================
#  INDIKATOR BARU v3.0
# ============================================================

def hitung_pivot_points(df) -> dict:
    """
    Pivot Points harian — Standard + Camarilla.
    Berdasarkan OHLC candle SEBELUMNYA (prev day).
    Populer untuk level intraday IDX.
    """
    if len(df) < 2:
        c = float(df['Close'].iloc[-1])
        return {'pp': c, 'r1': c, 'r2': c, 'r3': c,
                's1': c, 's2': c, 's3': c,
                'cr4': c, 'cr3': c, 'cs3': c, 'cs4': c}
    prev = df.iloc[-2]
    H = float(prev['High']); L = float(prev['Low']); C = float(prev['Close'])
    PP = (H + L + C) / 3
    r1 = 2 * PP - L;       s1 = 2 * PP - H
    r2 = PP + (H - L);     s2 = PP - (H - L)
    r3 = H + 2 * (PP - L); s3 = L - 2 * (H - PP)
    # Camarilla
    range_ = H - L
    cr4 = C + range_ * 1.1 / 2;  cs4 = C - range_ * 1.1 / 2
    cr3 = C + range_ * 1.1 / 4;  cs3 = C - range_ * 1.1 / 4
    return {
        'pp': PP, 'r1': r1, 'r2': r2, 'r3': r3,
        's1': s1, 's2': s2, 's3': s3,
        'cr4': cr4, 'cr3': cr3, 'cs3': cs3, 'cs4': cs4,
    }


def hitung_heikin_ashi(df):
    """
    Heikin Ashi candles — smoother trend, kurangi noise penny stocks IDX.
    Return: ha_o, ha_h, ha_l, ha_c (arrays), ha_bull_streak (int)
    """
    c = df['Close'].values; o = df['Open'].values
    h = df['High'].values;  l = df['Low'].values
    n = len(df)

    ha_c = (o + h + l + c) / 4
    ha_o = np.zeros(n)
    ha_o[0] = (o[0] + c[0]) / 2
    for i in range(1, n):
        ha_o[i] = (ha_o[i - 1] + ha_c[i - 1]) / 2
    ha_h = np.maximum(h, np.maximum(ha_o, ha_c))
    ha_l = np.minimum(l, np.minimum(ha_o, ha_c))

    # Jumlah candle HA berturut dari akhir (positif = bullish streak, negatif = bearish streak)
    ha_bull_streak = 0
    for i in range(n - 1, max(n - 10, -1), -1):
        if ha_c[i] > ha_o[i]:
            ha_bull_streak += 1
        else:
            break

    ha_bear_streak = 0
    for i in range(n - 1, max(n - 10, -1), -1):
        if ha_c[i] <= ha_o[i]:
            ha_bear_streak += 1
        else:
            break

    # Return signed streak: positif = bull, negatif = bear
    streak = ha_bull_streak if ha_bull_streak > 0 else -ha_bear_streak

    return ha_o, ha_h, ha_l, ha_c, streak


def hitung_squeeze_momentum(df, bb_len=20, bb_mult=2.0, kc_len=20, kc_mult=1.5):
    """
    Squeeze Momentum (LazyBear-style).
    sqz_on  = BB inside KC = konsolidasi sebelum ledak
    sqz_off = BB baru keluar KC = breakout baru mulai
    sqz_mom = arah momentum saat breakout (>0 = up, <0 = down)
    """
    c = df['Close'].values; h = df['High'].values; l = df['Low'].values
    n = len(df)
    if n < bb_len + 2:
        return {'sqz_on': False, 'sqz_off': False, 'sqz_mom': 0.0, 'no_sqz': True}

    # BB
    bb_mid_s = pd.Series(c).rolling(bb_len).mean()
    bb_std_s = pd.Series(c).rolling(bb_len).std()
    bb_up = (bb_mid_s + bb_mult * bb_std_s).values
    bb_dn = (bb_mid_s - bb_mult * bb_std_s).values

    # Keltner Channel (EMA + Wilder ATR)
    kc_mid = pd.Series(c).ewm(span=kc_len, adjust=False).mean().values
    atr_kc = hitung_atr_wilder(df, kc_len)
    kc_up  = kc_mid + kc_mult * atr_kc
    kc_dn  = kc_mid - kc_mult * atr_kc

    sqz_on_now  = bool((bb_up[-1] < kc_up[-1]) and (bb_dn[-1] > kc_dn[-1]))
    sqz_on_prev = bool((bb_up[-2] < kc_up[-2]) and (bb_dn[-2] > kc_dn[-2]))
    sqz_off_now = bool((bb_up[-1] > kc_up[-1]) and (bb_dn[-1] < kc_dn[-1]))

    # sqz_off = True hanya pada bar PERTAMA keluar squeeze (baru saja breakout)
    sqz_off = sqz_off_now and sqz_on_prev

    # Momentum oscillator: delta midpoint, linear regression slope 14 bar
    win = min(14, n)
    hp_s = pd.Series(h).rolling(kc_len).max().values
    lp_s = pd.Series(l).rolling(kc_len).min().values
    delta = c - ((hp_s + lp_s) / 2 + kc_mid) / 2
    y = delta[-win:]
    valid = ~np.isnan(y)
    if valid.sum() >= 3:
        xi = np.arange(len(y))[valid]
        yi = y[valid]
        slope = float(np.polyfit(xi, yi, 1)[0])
    else:
        slope = 0.0

    return {
        'sqz_on':  sqz_on_now,
        'sqz_off': sqz_off,
        'sqz_mom': slope,
        'no_sqz':  not sqz_on_now and not sqz_off_now,
    }


def hitung_atr_trailing_stop(df, mult=2.5, period=14):
    """
    ATR Trailing Stop — lebih responsif dari SuperTrend untuk scalp/swing.
    trail_trend: +1 = uptrend, -1 = downtrend.
    trail_dist_pct = jarak close ke trail stop dalam persen.
    """
    c   = df['Close'].values
    atr = hitung_atr_wilder(df, period)
    n   = len(df)
    if n < period + 1:
        return np.full(n, c[-1] if n else 0.0), np.zeros(n, int), 0.0

    trail = np.zeros(n)
    trail[period - 1] = c[period - 1] - mult * atr[period - 1]

    for i in range(period, n):
        stop_candidate = c[i] - mult * atr[i]
        if c[i] > trail[i - 1]:
            trail[i] = max(trail[i - 1], stop_candidate)
        else:
            trail[i] = stop_candidate

    trail_trend = np.where(c > trail, 1, -1)
    dist_pct = ((c[-1] - trail[-1]) / (c[-1] + 1e-10)) * 100

    return trail, trail_trend, float(dist_pct)


def hitung_volume_profile(df, period=20, bins=30) -> dict:
    """
    Volume Profile Lite — POC + Value Area (70% volume).
    POC = level harga dengan volume terbesar (magnet harga).
    va_high/va_low = range 70% konsentrasi volume.
    """
    df_tail = df.tail(period)
    p_min = float(df_tail['Low'].min())
    p_max = float(df_tail['High'].max())
    if p_max == p_min:
        return {'poc': p_min, 'va_high': p_min, 'va_low': p_min}

    edges = np.linspace(p_min, p_max, bins + 1)
    vol_profile = np.zeros(bins)

    for _, row in df_tail.iterrows():
        rh = float(row['High']); rl = float(row['Low']); rv = float(row['Volume'])
        cr = rh - rl
        if cr == 0:
            continue
        for j in range(bins):
            overlap = max(0.0, min(rh, edges[j + 1]) - max(rl, edges[j]))
            vol_profile[j] += rv * (overlap / cr)

    poc_idx = int(np.argmax(vol_profile))
    poc     = float((edges[poc_idx] + edges[poc_idx + 1]) / 2)

    # Value Area: expand dari POC sampai 70% total volume
    total_vol = float(vol_profile.sum())
    target    = 0.7 * total_vol
    cumvol    = float(vol_profile[poc_idx])
    lo_idx = hi_idx = poc_idx

    while cumvol < target and (lo_idx > 0 or hi_idx < bins - 1):
        add_lo = vol_profile[lo_idx - 1] if lo_idx > 0 else 0.0
        add_hi = vol_profile[hi_idx + 1] if hi_idx < bins - 1 else 0.0
        if add_lo >= add_hi and lo_idx > 0:
            lo_idx -= 1; cumvol += add_lo
        elif hi_idx < bins - 1:
            hi_idx += 1; cumvol += add_hi
        else:
            break

    return {
        'poc':     poc,
        'va_high': float(edges[hi_idx + 1]),
        'va_low':  float(edges[lo_idx]),
    }


def detect_rsi_divergence(df, lookback: int = 40, pivot_len: int = 3) -> dict:
    """
    Deteksi RSI Divergence:
    bull_div    = price LH + RSI HH → reversal up (regular bullish)
    bear_div    = price HH + RSI LH → reversal down (regular bearish)
    hidden_bull = price HL + RSI LL → trend continuation up (hidden bullish)
    """
    if len(df) < lookback + pivot_len * 2 + 2:
        return {'bull_div': False, 'bear_div': False, 'hidden_bull': False}

    c   = df['Close'].values[-lookback:]
    rsi = hitung_rsi(df, 14)[-lookback:]
    n   = len(c)

    # Cari swing lows dalam window
    price_lows = []
    for i in range(pivot_len, n - pivot_len):
        window_c = c[i - pivot_len: i + pivot_len + 1]
        if c[i] == window_c.min():
            price_lows.append((i, float(c[i]), float(rsi[i])))

    # Cari swing highs
    price_highs = []
    for i in range(pivot_len, n - pivot_len):
        window_c = c[i - pivot_len: i + pivot_len + 1]
        if c[i] == window_c.max():
            price_highs.append((i, float(c[i]), float(rsi[i])))

    bull_div = bear_div = hidden_bull = False

    if len(price_lows) >= 2:
        p1, p2 = price_lows[-2], price_lows[-1]
        # Regular bullish: harga turun (LH), RSI naik
        bull_div    = p2[1] < p1[1] and p2[2] > p1[2]
        # Hidden bullish: harga naik (HL), RSI turun
        hidden_bull = p2[1] > p1[1] and p2[2] < p1[2]

    if len(price_highs) >= 2:
        h1, h2 = price_highs[-2], price_highs[-1]
        # Regular bearish: harga naik (HH), RSI turun
        bear_div = h2[1] > h1[1] and h2[2] < h1[2]

    return {
        'bull_div':    bull_div,
        'bear_div':    bear_div,
        'hidden_bull': hidden_bull,
    }


# ============================================================
#  HETRIK PATTERN DETECTOR — 7-Candle Cycle Theory
# ============================================================
def detect_hetrik_pattern(df, min_streak: int = 3) -> dict:
    """
    Deteksi 7-Candle Cycle (Hetrik) dari Facebook post:
    C1-C2: Akumulasi  |  C3: Breakout (entry)
    C4: Exhaustion (vol↓ + dominant wick)
    C5: Retracement/liquidity grab (re-entry / exit)
    C6-C7: Continuation push

    Egy Setiawan shortcut:
        GRG = Green→Red→Green, close G2 > close R → entry setelah retest
        RGR = Red→Green→Red,   close R2 < close G → short setelah retest

    Returns dict dengan semua variabel hetrik.
    Aman dipanggil dari build_context() dan cmd_hetrik().
    """
    _empty = {
        'hetrik':             False,
        'hetrik_bull':        False,
        'hetrik_bear':        False,
        'hetrik_streak':      0,
        'hetrik_phase':       0,     # 3=C3 entry, 4=C4 exhaustion, 5=C5 retrace, 6-7=push
        'hetrik_score':       0,     # 0–6
        'hetrik_grg':         False, # Green-Red-Green (bull GRG / bear RGR)
        'hetrik_c4_exh':      False, # C4 exhaustion: vol↓ + dominant wick
        'hetrik_c4_wick':     0.0,   # C4 wick ratio (0–1)
        'hetrik_c5_retrace':  False, # C5 retracement confirmed
        'hetrik_entry_ref':   0.0,   # Close C3 → re-entry level
        'hetrik_vol_ratio':   0.0,   # C3 vol / avg20vol
    }
    if df is None or len(df) < 7:
        return _empty

    c = df['Close'].values
    o = df['Open'].values
    h = df['High'].values
    l = df['Low'].values
    v = df['Volume'].values
    n = len(df)

    colors = ['green' if c[i] >= o[i] else 'red' for i in range(n)]

    # ── Cari streak minimum min_streak yang berakhir di salah satu dari 5 candle terakhir ──
    phase       = 0
    streak_col  = None
    streak_len  = 0
    c3_idx      = -1    # index candle penutup streak (= "C3")

    for offset in range(min(5, n)):
        end_idx = n - 1 - offset
        col     = colors[end_idx]
        cnt     = 0
        for j in range(end_idx, max(-1, end_idx - 10), -1):
            if colors[j] == col:
                cnt += 1
            else:
                break
        if cnt < min_streak:
            continue
        # Pastikan ini bukan lanjutan streak yang lebih panjang
        pre_idx = end_idx - cnt
        if pre_idx >= 0 and colors[pre_idx] == col:
            continue   # masih ada candle sebelumnya → bukan awal streak
        # Streak valid ditemukan
        c3_idx     = end_idx
        streak_col = col
        streak_len = cnt
        phase      = 3 + offset    # offset=0 → C3, offset=1 → C4, dst.
        break

    if c3_idx == -1:
        return _empty

    # ── C4 Analysis ──────────────────────────────────────────────────────────
    c4_idx        = c3_idx + 1
    c4_exh        = False
    c4_wick_ratio = 0.0

    if c4_idx < n:
        c4_upper = h[c4_idx] - max(c[c4_idx], o[c4_idx])
        c4_lower = min(c[c4_idx], o[c4_idx]) - l[c4_idx]
        c4_range = h[c4_idx] - l[c4_idx] + 1e-9
        c4_wick_ratio = max(c4_upper, c4_lower) / c4_range
        c4_vol_ok  = v[c4_idx] < v[c3_idx]
        c4_wick_ok = c4_wick_ratio > 0.35
        c4_exh = c4_vol_ok and c4_wick_ok

    # ── C5 Retracement ───────────────────────────────────────────────────────
    c5_idx    = c3_idx + 2
    c5_retrace = False
    c3_close  = float(c[c3_idx])

    if c5_idx < n:
        if streak_col == 'green':
            # Retrace = candle merah atau close di bawah C3 close
            c5_retrace = (colors[c5_idx] == 'red') or (c[c5_idx] < c3_close)
        else:
            c5_retrace = (colors[c5_idx] == 'green') or (c[c5_idx] > c3_close)

    # ── GRG / RGR Pattern (Egy Setiawan) ────────────────────────────────────
    grg = False
    if n >= 3:
        grg = (
            colors[n-3] == 'green' and
            colors[n-2] == 'red'   and
            colors[n-1] == 'green' and
            c[n-1] > c[n-2]           # close hijau > close merah
        )
    rgr = False
    if n >= 3:
        rgr = (
            colors[n-3] == 'red'   and
            colors[n-2] == 'green' and
            colors[n-1] == 'red'   and
            c[n-1] < c[n-2]           # close merah < close hijau
        )

    # ── Volume Ratio C3 ──────────────────────────────────────────────────────
    va20 = float(pd.Series(v).rolling(20).mean().values[-1])
    c3_vr = float(v[c3_idx]) / va20 if va20 > 0 else 0.0

    # ── Score (0–6) ───────────────────────────────────────────────────────────
    score = 0
    if streak_len >= 3: score += 2
    if streak_len >= 4: score += 1   # bonus streak panjang
    if c4_exh:          score += 1
    if c5_retrace:      score += 1
    if grg or rgr:      score += 1

    is_bull = streak_col == 'green'

    return {
        'hetrik':            True,
        'hetrik_bull':       is_bull,
        'hetrik_bear':       not is_bull,
        'hetrik_streak':     streak_len,
        'hetrik_phase':      phase,
        'hetrik_score':      score,
        'hetrik_grg':        grg or rgr,
        'hetrik_c4_exh':     c4_exh,
        'hetrik_c4_wick':    round(c4_wick_ratio, 2),
        'hetrik_c5_retrace': c5_retrace,
        'hetrik_entry_ref':  round(c3_close, 2),
        'hetrik_vol_ratio':  round(c3_vr, 2),
    }


def fast_gate(df) -> bool:
    """
    Tier-1 gate untuk tiered scanning.
    Evaluasi 3 kriteria cepat saja (tanpa build_context penuh).
    ~85% ticker terfilter di sini → hemat CPU 60-70% di /swing & /scr.
    """
    if df is None or len(df) < 40:
        return False
    c = df['Close'].values; v = df['Volume'].values

    # Liquidity gate: min volume 500K dan harga > Rp50
    last_close = float(c[-1])
    if last_close < 50:
        return False  # penny stock ekstrem

    va20 = pd.Series(v).rolling(20).mean().values
    if np.isnan(va20[-1]) or va20[-1] == 0:
        return False
    if float(v[-1]) / float(va20[-1]) < 0.7:
        return False  # volume sangat sepi

    # EMA gate: EMA9 > EMA21 (minimal uptrend)
    e9  = float(df['Close'].ewm(span=9,  adjust=False).mean().iloc[-1])
    e21 = float(df['Close'].ewm(span=21, adjust=False).mean().iloc[-1])
    if e9 < e21 * 0.985:  # toleransi 1.5% agar tidak terlalu ketat
        return False

    # SuperTrend cepat (7, 1.7)
    _, _, st_trend = hitung_supertrend(df, 7, 1.7)
    return bool(st_trend[-1] == 1)


# ============================================================
#  FORMULA CONTEXT
# ============================================================
def _dummy_ctx() -> dict:
    fn = lambda *a: 1000.0
    return {
        'close': 1000, 'open': 990, 'high': 1010, 'low': 985,
        'volume': 5e6, 'mid_price': 997,
        'prev_close': 960, 'prev_open': 950, 'prev_high': 970, 'prev_low': 945,
        'prev_volume': 4e6,
        'prev2_close': 940, 'prev2_open': 930, 'prev2_high': 955, 'prev2_low': 925,
        'prev3_close': 920, 'prev3_high': 935, 'prev3_low': 910,
        'ema9': 995, 'prev_ema9': 990, 'ema21': 980, 'prev_ema21': 975,
        'ema50': 960, 'prev_ema50': 958, 'sma5': 993, 'prev_sma5': 988,
        'sma20': 982, 'prev_sma20': 978, 'sma26': 975, 'sma200': 900,
        'macd': 5, 'prev_macd': 3, 'macd_signal': 2, 'prev_macd_signal': 1,
        'macd_hist': 3, 'prev_macd_hist': 2, 'prev2_macd_hist': 1,
        'rsi': 60, 'prev_rsi': 55, 'stoch_k': 65, 'prev_stoch_k': 58,
        'stoch_d': 60, 'prev_stoch_d': 62, 'adx': 25, 'di_plus': 30, 'di_minus': 20,
        'bb_top': 1020, 'bb_bottom': 980, 'bb_mid': 1000, 'bb_bw': 0.04, 'bb_pct': 0.5,
        'prev_bb_top': 1018, 'prev_bb_bottom': 982, 'atr': 15, 'prev_atr': 14,
        'cci': 50, 'prev_cci': 40, 'williams_r': -35, 'prev_williams_r': -42,
        'mfi': 60, 'prev_mfi': 55, 'roc': 3.2, 'prev_roc': 2.1,
        'obv': 1e7, 'prev_obv': 9.5e6, 'sar': 975, 'vwap': 995, 'prev_vwap': 990,
        'vol_avg': 3e6, 'vol_ratio': 1.67,
        # ── v3.0: Pivot Points ───────────────────────────────────────────────────
        'pp': 990, 'r1': 1010, 'r2': 1030, 'r3': 1050,
        's1': 970, 's2': 950, 's3': 930,
        'cr4': 1025, 'cr3': 1008, 'cs3': 975, 'cs4': 958,
        # ── v3.0: Heikin Ashi ───────────────────────────────────────────────────
        'ha_close': 998, 'ha_open': 992, 'ha_bull_streak': 3,
        # ── v3.0: Squeeze Momentum ──────────────────────────────────────────────
        'sqz_on': False, 'sqz_off': False, 'sqz_mom': 0.5, 'no_sqz': True,
        'sq_val': 0.5,           # alias sqz_mom
        # ── v3.0: ATR Trailing Stop ─────────────────────────────────────────────
        'trail_stop': 970, 'trail_dist_pct': 3.1,
        # ── v3.0: Volume Profile ────────────────────────────────────────────────
        'poc': 988, 'va_high': 1005, 'va_low': 975,
        # ── v3.0: RSI Divergence ────────────────────────────────────────────────
        'bull_div': False, 'bear_div': False, 'hidden_bull': False,
        # ── v3.1 NEW: CMF ───────────────────────────────────────────────────────
        'cmf': 0.12, 'prev_cmf': 0.08,
        'cmf_bull': True, 'cmf_bear': False,
        # ── v3.1 NEW: Supply & Demand proximity ─────────────────────────────────
        'near_demand': False, 'near_supply': False,
        # ── v3.1 NEW: Volume spike ──────────────────────────────────────────────
        'vol_spike': True,
        # ── v3.1 NEW: HTF (Daily) ───────────────────────────────────────────────
        'htf_bull': True, 'htf_bear': False,
        'htf_rsi': 58.0, 'htf_cmf': 0.10,
        'htf_ema50': 950.0, 'htf_close': 1000.0,
        # ── v3.1 NEW: Confluence Scores ─────────────────────────────────────────
        'scl': 7, 'scs': 1,   # Scalp Long/Short score (max 9)
        'swl': 8, 'sws': 0,   # Swing Long/Short score (max 10)
        # ── v3.1 NEW: SuperTrend direction (int) ────────────────────────────────
        'st_trend': 1,         # 1 = bull, -1 = bear
        # ── v3.2 NEW: Hetrik 7-Candle Cycle ─────────────────────────────────────
        'hetrik': False, 'hetrik_bull': False, 'hetrik_bear': False,
        'hetrik_streak': 0, 'hetrik_phase': 0, 'hetrik_score': 0,
        'hetrik_grg': False, 'hetrik_c4_exh': False, 'hetrik_c4_wick': 0.0,
        'hetrik_c5_retrace': False, 'hetrik_entry_ref': 1000.0,
        'hetrik_vol_ratio': 1.0,
        # ── Lambdas ─────────────────────────────────────────────────────────────
        'sma': fn, 'ema': fn, 'hhv': fn, 'llv': fn,
        'hhv_close': fn, 'llv_close': fn, 'roc_n': fn, 'vol_sma': fn,
        'prev_c': fn, 'prev_h': fn, 'prev_l': fn,
        'atr_n': fn, 'rsi_n': fn, 'cci_n': fn, 'mfi_n': fn,
        'True': True, 'False': False,
    }

def build_context(df: pd.DataFrame, ticker: str = None,
                  intraday: bool = False,
                  needed_groups: set = None) -> dict | None:
    """
    Build formula evaluation context.
    needed_groups=None  → hitung semua (full mode: /scan /swing /algo)
    needed_groups={...} → hitung hanya grup yang diperlukan (lazy mode: /scr)
    Lazy mode hemat 40-70% CPU tergantung formula.
    """
    if df is None or len(df) < 40:
        return None

    _lazy = needed_groups is not None
    _need = needed_groups if _lazy else set(_IND_GROUPS.keys())

    # Daily: buang candle hari ini jika market sedang buka
    if not intraday:
        _now = datetime.now(WIB)
        _market_open = (_now.weekday() < 5 and
                        9 * 60 <= _now.hour * 60 + _now.minute <= 15 * 60 + 30)
        if _market_open and len(df) >= 41:
            df = df.iloc[:-1].copy()

    c = df['Close'].values; o = df['Open'].values
    h = df['High'].values;  l = df['Low'].values; v = df['Volume'].values

    def _g(arr, off=0):
        return float(arr[-1 - int(off)]) if abs(-1 - int(off)) <= len(arr) else 0.0
    def _z(): return np.zeros(len(df))

    # ── SELALU dihitung (murah + dibutuhkan banyak grup) ────────────────────
    _va20 = pd.Series(v).rolling(20).mean().values
    va    = float(_va20[-1]) if not np.isnan(_va20[-1]) else 1.0
    _vol_spike = (va > 0 and float(v[-1]) / va >= 1.5)
    _e9   = hitung_ema(df, 9)
    _e21  = hitung_ema(df, 21)
    _e50  = hitung_ema(df, 50)
    _rsi  = hitung_rsi(df, 14)
    _cmf  = hitung_cmf(df, 20)
    _, _, _st_tr = hitung_supertrend(df, ATR_PERIOD, ST_MULTIPLIER)
    _st_trend    = int(_st_tr[-1])

    # ── CONDITIONAL groups ───────────────────────────────────────────────────
    _s5 = _s20 = _s26 = _s200 = _z()
    if 'sma' in _need:
        _s5 = hitung_sma(df,5); _s20 = hitung_sma(df,20)
        _s26 = hitung_sma(df,26); _s200 = hitung_sma(df,200)

    _mac = _ms = _mh = _z()
    if 'macd' in _need:
        _mac, _ms, _mh = hitung_macd(df)

    _sk = _sd2 = _z()
    if 'stoch' in _need:
        _sk, _sd2 = hitung_stochastic(df, 15, 3, 3)

    _adx = _dip = _dim = _z()
    if 'adx' in _need:
        _adx, _dip, _dim = hitung_adx(df, 14)

    _bbt = _bbb = _bbm = _bbbw = _bbpb = _z()
    if 'bb' in _need or 'sqz' in _need:
        _bbt, _bbb, _bbm, _bbbw, _bbpb = hitung_bollinger(df, 20, 2.0)

    _atr_arr = _z()
    if 'atr' in _need or 'trail' in _need or 'sqz' in _need or 'sd' in _need:
        _atr_arr = hitung_atr_sma(df, 14)

    _cci = _z()
    if 'cci' in _need:
        _cci = hitung_cci(df, 14)

    _wr = _z()
    if 'williams' in _need:
        _wr = hitung_williams_r(df, 14)

    _mfi = _z()
    if 'mfi' in _need:
        _mfi = hitung_mfi(df, 14)

    _roc = _z()
    if 'roc' in _need:
        _roc = hitung_roc(df, 12)

    _obv = _z()
    if 'obv' in _need:
        _obv = hitung_obv(df)

    _sar = _z()
    if 'sar' in _need:
        _sar = hitung_sar(df)

    _vwap = _z()
    if 'vwap' in _need:
        _vwap = hitung_vwap_daily(df)

    _pvt = {'pp':0,'r1':0,'r2':0,'r3':0,'s1':0,'s2':0,'s3':0,
            'cr4':0,'cr3':0,'cs3':0,'cs4':0}
    if 'pivot_pts' in _need:
        _pvt = hitung_pivot_points(df)

    _ha_o = _ha_c = _z(); _ha_streak = 0
    if 'ha' in _need:
        _ha_o, _, _, _ha_c, _ha_streak = hitung_heikin_ashi(df)

    _sqz = {'sqz_on':False,'sqz_off':False,'sqz_mom':0.0,'no_sqz':False}
    if 'sqz' in _need:
        _sqz = hitung_squeeze_momentum(df)

    _trail = _z(); _trail_dist = 0.0
    if 'trail' in _need:
        _trail, _, _trail_dist = hitung_atr_trailing_stop(df, mult=2.5, period=14)

    _vp = {'poc':0.0,'va_high':0.0,'va_low':0.0}
    if 'vp' in _need:
        _vp = hitung_volume_profile(df, period=20, bins=30)

    _div = {'bull_div':False,'bear_div':False,'hidden_bull':False}
    if 'div' in _need:
        _div = detect_rsi_divergence(df)

    _hetrik_empty = {
        'hetrik':False,'hetrik_bull':False,'hetrik_bear':False,
        'hetrik_streak':0,'hetrik_phase':0,'hetrik_score':0,
        'hetrik_grg':False,'hetrik_c4_exh':False,'hetrik_c4_wick':0.0,
        'hetrik_c5_retrace':False,'hetrik_entry_ref':0.0,'hetrik_vol_ratio':0.0,
    }
    _hetrik = detect_hetrik_pattern(df) if 'hetrik' in _need else _hetrik_empty

    _near_demand = _near_supply = False
    if 'sd' in _need:
        _phi, _plo = hitung_pivot(df, PIVOT_LENGTH)
        _valid_phi = _phi[~np.isnan(_phi)]; _valid_plo = _plo[~np.isnan(_plo)]
        _atr_w     = hitung_atr_wilder(df, ATR_PERIOD)
        _near_buf  = float(_atr_w[-1]) * NEAR_FACTOR
        _close_now = float(c[-1])
        _last_sup  = float(_valid_plo[-1]) if len(_valid_plo) > 0 else None
        _last_res  = float(_valid_phi[-1]) if len(_valid_phi) > 0 else None
        _near_demand = (_last_sup is not None and
                        _close_now >= _last_sup - _near_buf and
                        _close_now <= _last_sup + _near_buf)
        _near_supply = (_last_res is not None and
                        _close_now >= _last_res - _near_buf and
                        _close_now <= _last_res + _near_buf)

    _htf = {'htf_bull':False,'htf_bear':False,'htf_rsi':50.0,
            'htf_cmf':0.0,'htf_ema50':0.0,'htf_close':0.0}
    if 'htf' in _need and ticker and not intraday:
        if len(df) >= 60:
            try:
                _, _, _st_htf = hitung_supertrend(df, ATR_PERIOD, ST_MULTIPLIER)
                _ema50_htf   = hitung_ema(df, 50)
                _rsi_htf     = hitung_rsi(df, 14)
                _cmf_htf     = hitung_cmf(df, 20)
                _close_htf   = float(df['Close'].values[-1])
                _e50_htf     = float(_ema50_htf[-1])
                _htf = {
                    'htf_bull':  bool(_st_htf[-1] == 1) and _close_htf > _e50_htf,
                    'htf_bear':  bool(_st_htf[-1] == -1) and _close_htf < _e50_htf,
                    'htf_rsi':   float(_rsi_htf[-1]),
                    'htf_cmf':   float(_cmf_htf[-1]),
                    'htf_ema50': _e50_htf,
                    'htf_close': _close_htf,
                }
            except Exception as _ex:
                logger.debug("build_context HTF: %s", _ex)
        else:
            _htf = hitung_htf_context(ticker)

    ctx = {
        'close': float(c[-1]), 'open': float(o[-1]), 'high': float(h[-1]),
        'low': float(l[-1]), 'volume': float(v[-1]), 'mid_price': float((h[-1]+l[-1])/2),
        'prev_close': _g(c,1), 'prev_open': _g(o,1), 'prev_high': _g(h,1),
        'prev_low': _g(l,1), 'prev_volume': _g(v,1),
        'prev2_close': _g(c,2), 'prev2_open': _g(o,2),
        'prev2_high': _g(h,2), 'prev2_low': _g(l,2),
        'prev3_close': _g(c,3), 'prev3_high': _g(h,3), 'prev3_low': _g(l,3),
        'ema9': _g(_e9), 'prev_ema9': _g(_e9,1),
        'ema21': _g(_e21), 'prev_ema21': _g(_e21,1),
        'ema50': _g(_e50), 'prev_ema50': _g(_e50,1),
        'sma5': _g(_s5), 'prev_sma5': _g(_s5,1),
        'sma20': _g(_s20), 'prev_sma20': _g(_s20,1),
        'sma26': _g(_s26), 'sma200': _g(_s200),
        'macd': _g(_mac), 'prev_macd': _g(_mac,1),
        'macd_signal': _g(_ms), 'prev_macd_signal': _g(_ms,1),
        'macd_hist': _g(_mh), 'prev_macd_hist': _g(_mh,1), 'prev2_macd_hist': _g(_mh,2),
        'rsi': _g(_rsi), 'prev_rsi': _g(_rsi,1),
        'stoch_k': _g(_sk), 'prev_stoch_k': _g(_sk,1),
        'stoch_d': _g(_sd2), 'prev_stoch_d': _g(_sd2,1),
        'adx': _g(_adx), 'di_plus': _g(_dip), 'di_minus': _g(_dim),
        'bb_top': _g(_bbt), 'prev_bb_top': _g(_bbt,1),
        'bb_bottom': _g(_bbb), 'prev_bb_bottom': _g(_bbb,1),
        'bb_mid': _g(_bbm), 'bb_bw': _g(_bbbw), 'bb_pct': _g(_bbpb),
        'atr': _g(_atr_arr), 'prev_atr': _g(_atr_arr,1),
        'cci': _g(_cci), 'prev_cci': _g(_cci,1),
        'williams_r': _g(_wr), 'prev_williams_r': _g(_wr,1),
        'mfi': _g(_mfi), 'prev_mfi': _g(_mfi,1),
        'roc': _g(_roc), 'prev_roc': _g(_roc,1),
        'obv': _g(_obv), 'prev_obv': _g(_obv,1),
        'sar': _g(_sar), 'vwap': _g(_vwap), 'prev_vwap': _g(_vwap,1),
        'vol_avg': va, 'vol_ratio': (float(v[-1]) / va if va else 0.0),
        'pp': _pvt['pp'], 'r1': _pvt['r1'], 'r2': _pvt['r2'], 'r3': _pvt['r3'],
        's1': _pvt['s1'], 's2': _pvt['s2'], 's3': _pvt['s3'],
        'cr4': _pvt['cr4'], 'cr3': _pvt['cr3'], 'cs3': _pvt['cs3'], 'cs4': _pvt['cs4'],
        'ha_close': float(_ha_c[-1]) if 'ha' in _need else 0.0,
        'ha_open':  float(_ha_o[-1]) if 'ha' in _need else 0.0,
        'ha_bull_streak': int(_ha_streak),
        'sqz_on': _sqz['sqz_on'], 'sqz_off': _sqz['sqz_off'],
        'sqz_mom': _sqz['sqz_mom'], 'no_sqz': _sqz['no_sqz'],
        'trail_stop':     float(_trail[-1]) if 'trail' in _need else 0.0,
        'trail_dist_pct': _trail_dist,
        'poc': _vp['poc'], 'va_high': _vp['va_high'], 'va_low': _vp['va_low'],
        'bull_div': _div['bull_div'], 'bear_div': _div['bear_div'],
        'hidden_bull': _div['hidden_bull'],
        'cmf':      float(_cmf[-1]),
        'prev_cmf': float(_cmf[-2]) if len(_cmf) >= 2 else 0.0,
        'cmf_bull': float(_cmf[-1]) >  0.05,
        'cmf_bear': float(_cmf[-1]) < -0.05,
        'near_demand': _near_demand, 'near_supply': _near_supply,
        'vol_spike':   _vol_spike,   'st_trend':    _st_trend,
        'sq_val':      _sqz['sqz_mom'],
        **_htf, **_hetrik,
        'sma':       lambda p: float(df['Close'].rolling(int(p)).mean().iloc[-1]),
        'ema':       lambda p: float(df['Close'].ewm(span=int(p), adjust=False).mean().iloc[-1]),
        'hhv':       lambda p: float(df['High'].rolling(int(p)).max().iloc[-1]),
        'llv':       lambda p: float(df['Low'].rolling(int(p)).min().iloc[-1]),
        'hhv_close': lambda p: float(df['Close'].rolling(int(p)).max().iloc[-1]),
        'llv_close': lambda p: float(df['Close'].rolling(int(p)).min().iloc[-1]),
        'roc_n':     lambda p: float((c[-1]-c[-1-int(p)])/c[-1-int(p)]*100)
                                if len(c) > int(p) and c[-1-int(p)] != 0 else 0.0,
        'vol_sma':   lambda p: float(pd.Series(v).rolling(int(p)).mean().iloc[-1]),
        'prev_c':    lambda n: float(c[-1-int(n)]) if len(c) > int(n) else float(c[0]),
        'prev_h':    lambda n: float(h[-1-int(n)]) if len(h) > int(n) else float(h[0]),
        'prev_l':    lambda n: float(l[-1-int(n)]) if len(l) > int(n) else float(l[0]),
        'atr_n':     lambda p: float(pd.Series(hitung_true_range(df)).rolling(int(p)).mean().iloc[-1]),
        'rsi_n':     lambda p: float(hitung_rsi(df, int(p))[-1]),
        'cci_n':     lambda p: float(hitung_cci(df, int(p))[-1]),
        'mfi_n':     lambda p: float(hitung_mfi(df, int(p))[-1]),
        'True': True, 'False': False,
    }

    if not _lazy or 'confluence' in _need:
        _cf = hitung_confluence(ctx)
        ctx.update(_cf)
    else:
        ctx.update({'scl': 0, 'scs': 0, 'swl': 0, 'sws': 0})

    return ctx


# ============================================================
def safe_download(ticker: str, interval: str, period: str) -> pd.DataFrame | None:
    key = _cache_key('sdl', ticker, interval, period)
    cached, hit = _yf_cache_get(key)
    if hit:
        return cached

    def _do():
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            with contextlib.redirect_stdout(_io_sup.StringIO()), \
                 contextlib.redirect_stderr(_io_sup.StringIO()):
                df = yf.download(kode_yf(ticker), interval=interval, period=period,
                                 progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 20:
            return None
        df = df.dropna()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df if len(df) >= 20 else None

    _do.__name__ = f'yf_dl_{ticker}_{interval}'
    result = yf_safe_call(_do, ttl=_YF_CACHE_TTL)
    return result

def fmp_fetch_kline(ticker: str, interval: str = "1d", period: str = "6mo") -> 'pd.DataFrame | None':
    """
    Fetch OHLCV dari Financial Modeling Prep (FMP).
    Mendukung format saham IDX dengan suffix .JK
    """
    if not FMP_API_KEY:
        return None

    key = _cache_key('fmp_kl', ticker, interval, period)
    cached, hit = _yf_cache_get(key)
    if hit:
        return cached

    symbol = f"{ticker}.JK"
    fmp_int_map = {'1m': '1min', '5m': '5min', '15m': '15min', '30m': '30min', '60m': '1hour', '1h': '1hour'}
    
    try:
        if interval == '1d':
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}"
            params = {"apikey": FMP_API_KEY}
        else:
            fmp_int = fmp_int_map.get(interval, '1hour')
            url = f"https://financialmodelingprep.com/api/v3/historical-chart/{fmp_int}/{symbol}"
            params = {"apikey": FMP_API_KEY}

        r = _http.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return None
        
        raw = r.json()
        if not raw:
            return None

        # Format JSON FMP berbeda antara endpoint daily dan intraday
        data_list = raw.get("historical", raw) if interval == '1d' else raw
        
        if not isinstance(data_list, list) or len(data_list) == 0:
            return None

        rows = []
        for item in data_list:
            rows.append({
                "Datetime": pd.to_datetime(item["date"]),
                "Open": float(item["open"]),
                "High": float(item["high"]),
                "Low": float(item["low"]),
                "Close": float(item["close"]),
                "Volume": float(item["volume"])
            })

        df = pd.DataFrame(rows).set_index("Datetime").sort_index()
        
        # Konversi Timezone ke WIB agar seragam dengan fungsi chart
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC').tz_convert(WIB)
        elif str(df.index.tz) != 'Asia/Jakarta':
            df.index = df.index.tz_convert(WIB)

        if len(df) >= 20:
            ttl = 60 if interval != '1d' else _YF_CACHE_TTL
            _yf_cache_set(key, df, ttl)
            return df
            
        return None
    except Exception as e:
        logger.debug("fmp_fetch_kline %s error: %s", ticker, e)
        return None

def safe_download_scan_fallback(ticker: str, interval: str, period: str) -> 'pd.DataFrame | None':
    """
    Download OHLCV untuk /scan: Twelve Data dulu → Yahoo Finance fallback.
    TD: akurat untuk IDX, tidak kena rate-limit Yahoo.
    """
    td_int_map = {
        '1m': '1min', '5m': '5min', '15m': '15min',
        '30m': '30min', '60m': '1h', '1h': '1h', '1d': '1day'
    }
    td_int   = td_int_map.get(interval, '1day')
    td_count = TD_COUNT_MAP.get(period, 200)
    df = td_fetch_kline(ticker, td_int, td_count)
    if df is not None:
        logger.info("SCAN_SRC: Twelve Data -> %s", ticker)
        return df
    logger.info("SCAN_SRC: Yahoo Finance -> %s", ticker)
    return safe_download(ticker, interval, period)

# ============================================================
#  IDX.co.id — Universe Discovery (backup itick /stock/rank)
# ============================================================
def idx_fetch_active_stocks(top_n: int = 400) -> 'list[str] | None':
    """
    Ambil daftar saham paling aktif dari IDX.co.id official endpoint.
    Tidak butuh auth — public API.
    Return list ticker IDX tanpa suffix, sorted by volume desc.
    """
    url = "https://idx.co.id/primary/TradingData/GetStockSummary"
    params = {
        "start": 0, "length": 9999, "draw": 1,
        "searchData": "", "sortColumn": "Volume", "sortType": "desc",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://idx.co.id/",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        r = _http.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json().get("data", [])
        idx_set = set(IDX_STOCKS)
        result: list[str] = []
        for row in data:
            kode = row.get("StockCode", "").strip().upper()
            vol  = row.get("Volume", 0) or 0
            freq = row.get("Frequency", 0) or 0
            # Filter: ada di IDX_STOCKS + volume > 0 + freq > 100 (likuid)
            if kode and kode in idx_set and vol > 0 and freq > 100:
                result.append(kode)
            if len(result) >= top_n:
                break
        logger.info("idx.co.id universe: %d kandidat", len(result))
        return result if len(result) >= 30 else None
    except Exception as e:
        logger.warning("idx_fetch_active_stocks error: %s", e)
        return None


# ============================================================
#  Twelve Data — OHLCV fetch (backup itick kline)
# ============================================================
def td_fetch_kline(ticker: str, interval: str = "1day",
                   outputsize: int = 200) -> 'pd.DataFrame | None':
    """
    Fetch OHLCV dari Twelve Data.
    interval: '1min','5min','15min','30min','1h','1day','1week'
    ticker IDX: 'BBCA' → API param 'BBCA/IDX'
    Free tier: 800 req/day, 8 req/min.
    """
    if not TWELVEDATA_API_KEY:
        return None
    key = _cache_key('td_kl', ticker, interval, outputsize)
    cached, hit = _yf_cache_get(key)
    if hit:
        return cached
    ttl = 60 if interval not in ('1day', '1week') else _YF_CACHE_TTL
    url = f"{TWELVEDATA_BASE_URL}/time_series"
    params = {
        "symbol":     ticker,       # tanpa suffix — mic_code sebagai param terpisah
        "mic_code":   "XIDX",       # MIC code resmi Bursa Efek Indonesia
        "interval":   interval,
        "outputsize": outputsize,
        "apikey":     TWELVEDATA_API_KEY,
        "format":     "JSON",
        "order":      "ASC",
        "dp":         2,
    }
    try:
        r   = _http.get(url, params=params, timeout=15)
        raw = r.json()
        if raw.get("status") == "error" or "values" not in raw:
            msg = raw.get("message", "no values")
            # Suppress spam pesan plan/pricing TwelveData — tidak perlu muncul tiap scan
            if "available starting" in msg.lower() or "upgrade" in msg.lower() or "plan" in msg.lower():
                logger.debug("td_fetch_kline %s: plan restricted (suppressed)", ticker)
            else:
                logger.warning("td_fetch_kline %s: %s", ticker, msg)
            return None
        df = pd.DataFrame(raw["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.set_index("datetime")
        df = df.rename(columns={
            "open": "Open", "high": "High",
            "low":  "Low",  "close": "Close", "volume": "Volume"
        })
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna()
        if len(df) >= 20:
            _yf_cache_set(key, df, ttl)
            return df
        return None
    except Exception as e:
        logger.debug("td_fetch_kline %s: %s", ticker, e)
        return None


async def td_batch_fetch(tickers: list, interval: str = "1day",
                          outputsize: int = 200, n_workers: int = 8) -> dict:
    """
    Parallel fetch dari Twelve Data.
    Free tier: 800 req/day, 8 req/min → throttle 0.08s antar request.
    n_workers=8 optimal untuk free tier.
    """
    results: dict = {}
    loop = asyncio.get_event_loop()
    sem  = asyncio.Semaphore(n_workers)

    async def _one(t: str):
        async with sem:
            df = await loop.run_in_executor(
                _IO_POOL, td_fetch_kline, t, interval, outputsize)
            if df is not None and len(df) >= 40:
                results[t] = df
            await asyncio.sleep(7.5)   # TD free tier: 8 req/min = 7.5s gap

    await asyncio.gather(*[_one(t) for t in tickers], return_exceptions=True)
    return results


SCAN_BATCH = 50
SCAN_DELAY = float(os.getenv('SCAN_DELAY', 0.05))   # detik antar batch-group (default 0.05)


# ── Financial Modeling Prep (FMP) API ─────────────────────────────────────────
FMP_API_KEY = os.getenv('FMP_API_KEY', 'hsYYQ501zb9ZJgvu9FkBJ8EU2Sns4Dga')

# ── Twelve Data API ───────────────────────────────────────────────────────────
# Daftar free: twelvedata.com — 800 req/day, 8 req/min
# IDX stocks: symbol = "BBCA/IDX" (tanpa .JK)
TWELVEDATA_API_KEY  = os.getenv('TWELVEDATA_API_KEY', '5d6e6942df6d4374b5447badedda76fb')
TWELVEDATA_BASE_URL = "https://api.twelvedata.com"
# Bar count map: yf period string → TD outputsize
TD_COUNT_MAP = {'5d': 120, '15d': 200, '1mo': 45, '3mo': 90,
                '6mo': 180, '1y': 265, '2y': 530}
# TF map: yf/itick format → twelvedata interval
TD_TF_MAP = {'1': '1min', '5': '5min', '15': '15min', '30': '30min',
             '60': '1h', 'D': '1day', 'W': '1week', 'M': '1month'}

async def batch_download_multi(tickers: list, interval: str, period: str) -> dict:
    """Download N ticker sekaligus via yf_safe_call (cache + dedup). Cache 5 menit."""
    loop = asyncio.get_event_loop()

    def _do():
        yf_tickers = [kode_yf(t) for t in tickers]

        def _dl():
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with contextlib.redirect_stdout(_io_sup.StringIO()), \
                     contextlib.redirect_stderr(_io_sup.StringIO()):
                    return yf.download(
                        yf_tickers, interval=interval, period=period,
                        progress=False, auto_adjust=True,
                        group_by='ticker', threads=True
                    )

        # FIX #1: sertakan hash ticker list agar tiap batch punya cache key unik.
        # Bug lama: semua batch 50-ticker interval='1d' share key yg sama →
        # batch 1+ mengembalikan data batch 0 → 94% saham tidak dievaluasi.
        import hashlib as _hl
        _tick_sig = _hl.md5(str(sorted(yf_tickers)).encode()).hexdigest()[:10]
        _dl.__name__ = f'yf_batch_{interval}_{period}_{_tick_sig}'
        # Daily data tidak berubah dalam 30 menit → TTL lebih panjang = cache tetap valid
        # dari pre-warm 08:55 sampai user /swing atau /scr jam 09:00–09:25
        _ttl = 1800 if interval == '1d' else _YF_CACHE_TTL   # 30 min daily, 5 min intraday
        raw = yf_safe_call(_dl, ttl=_ttl)
        if raw is None:
            return {}

        result = {}
        single = (len(tickers) == 1)
        lvl0 = raw.columns.get_level_values(0) if not single else []
        for ticker, yft in zip(tickers, yf_tickers):
            try:
                df = raw.copy() if single else (raw[yft].copy() if yft in lvl0 else None)
                if df is None or df.empty:
                    continue
                df = df.dropna()
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                if len(df) >= 20:
                    result[ticker] = df
            except Exception:
                pass
        return result

    return await loop.run_in_executor(_IO_POOL, _do)

# ============================================================
#  PARALLEL BATCH PREFETCH — download N batch sekaligus
# ============================================================
async def prefetch_parallel(batches: list, interval: str, period: str,
                             n_parallel: int = 6) -> dict:
    """
    Download semua batch secara paralel dalam grup N_PARALLEL.
    n_parallel=6 optimal untuk Termux 8GB 5G.
    Naikkan ke 8 jika jaringan sangat cepat; turunkan ke 4 jika banyak error 429.
    """
    all_dfs: dict = {}
    for i in range(0, len(batches), n_parallel):
        group = batches[i:i + n_parallel]
        results = await asyncio.gather(
            *[batch_download_multi(b, interval, period) for b in group],
            return_exceptions=True
        )
        for r in results:
            if isinstance(r, dict):
                all_dfs.update(r)
        if i + n_parallel < len(batches):
            await asyncio.sleep(SCAN_DELAY)
    return all_dfs


# ============================================================
#  CORE ANALISIS — SnR + SuperTrend
# ============================================================
def analisis_snr(ticker: str, tf: str):
    tf_map = {
        '5m': ('5m', '5d'), '15m': ('15m', '15d'),
        '1h': ('60m', '60d'), '1d': ('1d', '1y')
    }
    if tf not in tf_map: return None, "TF tidak valid (5m/15m/1h/1d)"
    iv, per = tf_map[tf]
    # Menggunakan pipeline fallback baru: AV -> TD -> YF
    df = safe_download_scan_fallback(ticker, iv, per)
    if df is None: return None, "Data tidak cukup atau seluruh penyedia API (AV, TD, YF) gagal."

    atr = hitung_atr_wilder(df, ATR_PERIOD)   # fix: Pine pakai Wilder bukan SMA
    up, dn, trend = hitung_supertrend(df, ATR_PERIOD, ST_MULTIPLIER)
    phi, plo = hitung_pivot(df, PIVOT_LENGTH)
    lr = lp = np.nan
    lres = np.full(len(df), np.nan); lsup = np.full(len(df), np.nan)
    for i in range(len(df)):
        if not np.isnan(phi[i]): lr = phi[i]
        if not np.isnan(plo[i]): lp = plo[i]
        lres[i] = lr; lsup[i] = lp
    vhi = phi[~np.isnan(phi)]; vlo = plo[~np.isnan(plo)]
    res = float(vhi[-1]) if len(vhi) > 0 else None
    sup = float(vlo[-1]) if len(vlo) > 0 else None

    va = df['Volume'].rolling(VOL_WINDOW).mean().values
    vc = df['Volume'].values > (va * VOL_MINIMUM)
    bk = np.zeros(len(df), bool); sk = np.zeros(len(df), bool)
    bb = np.zeros(len(df), bool); sb = np.zeros(len(df), bool)
    hv = df['High'].values; lv = df['Low'].values
    for i in range(1, len(df)):
        # fix: directional check (Pine: low <= support + atr*0.5)
        nd = lv[i] <= lsup[i] + atr[i] * NEAR_FACTOR if not np.isnan(lsup[i]) else False
        ns = hv[i] >= lres[i] - atr[i] * NEAR_FACTOR if not np.isnan(lres[i]) else False
        # fix: sinyal hanya pada candle FLIP (sama dengan Pine buySignal/sellSignal)
        flip_up = trend[i] == 1  and trend[i - 1] == -1
        flip_dn = trend[i] == -1 and trend[i - 1] == 1
        if flip_up:
            if nd and vc[i]: bk[i] = True   # BUY Konfirmasi
            else:            bb[i] = True   # BUY Biasa
        if flip_dn:
            if ns and vc[i]: sk[i] = True   # SELL Konfirmasi
            else:            sb[i] = True   # SELL Biasa

    return {
        'ticker': ticker.upper(), 'tf': tf, 'close': float(df['Close'].iloc[-1]),
        'trend': trend[-1], 'st_line': float(up[-1] if trend[-1] == 1 else dn[-1]),
        'resistance': res, 'support': sup, 'atr': float(atr[-1]), 'vol_cukup': bool(vc[-1]),
        'buy_kuat': bool(bk[-1]), 'sell_kuat': bool(sk[-1]),
        'buy_biasa': bool(bb[-1]), 'sell_biasa': bool(sb[-1]),
        'rsi':       float(hitung_rsi(df, 14)[-1]),
        'adx':       float(hitung_adx(df, 14)[0][-1]),
        'mfi':       float(hitung_mfi(df, 14)[-1]),
        'ema9':      float(hitung_ema(df, 9)[-1]),
        'ema21':     float(hitung_ema(df, 21)[-1]),
        'macd_hist': float(hitung_macd(df)[2][-1]),
        'vwap':      float(hitung_vwap_daily(df)[-1]),
        '_df': df, '_up': up, '_dn': dn, '_trend': trend,
        '_buy_k': bk, '_sell_k': sk, '_buy_b': bb, '_sell_b': sb,
        '_atr_arr': atr, '_ema9': hitung_ema(df, 9), '_vwap': hitung_vwap_daily(df),
    }, None

# ============================================================
#  SCREENER ENGINE — swing & scalping
# ============================================================
def dsi_screener(ticker: str, mode: str):
    df = safe_download(ticker, '5m' if mode == 'scalping' else '1d',
                       '5d' if mode == 'scalping' else '6mo')
    if df is None or len(df) < 40: return None, False
    c = df['Close'].values; v = df['Volume'].values
    va = pd.Series(v).rolling(20).mean().values
    if np.isnan(va[-1]) or va[-1] == 0: return None, False
    vr = float(v[-1]) / float(va[-1])
    e9 = hitung_ema(df, 9); e21 = hitung_ema(df, 21)
    _, _, mh = hitung_macd(df); sk, sd = hitung_stochastic(df, 15, 3, 3)
    rsi = hitung_rsi(df, 14); adx, _, _ = hitung_adx(df, 14)
    vwap = hitung_vwap_daily(df)
    if any(np.isnan(x[-1]) for x in [e9, e21, mh, sk, sd, rsi, adx, vwap]):
        return None, False

    if mode == 'scalping':
        ok = (c[-1] > vwap[-1] and e9[-1] > e21[-1] and c[-1] > e9[-1]
              and mh[-1] > 0 and vr >= 1.5 and 50 < rsi[-1] < 75)
    else:
        n = len(df)
        ok = (c[-1] > e21[-1] and mh[-1] > 0 and mh[-1] > mh[-2]
              and (sk[-1] > sd[-1] or sk[-1] > 50) and vr >= 1.5
              and adx[-1] > 20 and (c[-1] > c[-6] if n >= 6 else True))

    if ok:
        return {'ticker': ticker.upper(), 'close': float(c[-1]),
                'vol_ratio': vr, 'rsi': float(rsi[-1]), 'adx': float(adx[-1])}, True
    return None, False

def dsi_screener_df(ticker: str, df, mode: str):
    """
    Evaluasi screener dari DataFrame pre-fetched.
    Swing v2: 10 kriteria berlapis + scoring 0-10, lolos jika score >= 6.
    Scalping: kriteria ketat intraday + SuperTrend wajib bullish.
    """
    if df is None or len(df) < 40: return None, False
    c = df['Close'].values; v = df['Volume'].values; n = len(df)
    va = pd.Series(v).rolling(20).mean().values
    if np.isnan(va[-1]) or va[-1] == 0: return None, False
    vr = float(v[-1]) / float(va[-1])

    e9  = hitung_ema(df, 9)
    e21 = hitung_ema(df, 21)
    e50 = hitung_ema(df, 50) if n >= 55 else e21
    ml, ms, mh = hitung_macd(df)
    sk, sd      = hitung_stochastic(df, 14, 3, 3)
    rsi         = hitung_rsi(df, 14)
    adx, dip, dim = hitung_adx(df, 14)
    vwap        = hitung_vwap_daily(df)
    _, bb_bot, bb_mid, bb_bw, _ = hitung_bollinger(df, 20, 2.0)
    mfi         = hitung_mfi(df, 14)
    obv         = hitung_obv(df)
    atr         = hitung_atr_wilder(df, 14)

    checks = [e9, e21, mh, sk, sd, rsi, adx, vwap, mfi]
    if any(np.isnan(x[-1]) for x in checks): return None, False

    _, _, st_trend = hitung_supertrend(df, 7, 1.7)
    st_bull = bool(st_trend[-1] == 1)

    # CMF(20) — dihitung sekali, dipakai swing & scalping
    _cmf_arr  = hitung_cmf(df, 20)
    _cmf_val  = float(_cmf_arr[-1]) if not np.isnan(_cmf_arr[-1]) else 0.0

    if mode == 'scalping':
        ok = (c[-1] > vwap[-1] and e9[-1] > e21[-1] and c[-1] > e9[-1]
              and mh[-1] > 0 and vr >= 1.5 and 50 < rsi[-1] < 75 and st_bull)
        if ok:
            return {
                'ticker': ticker.upper(), 'close': float(c[-1]),
                'vol_ratio': vr, 'rsi': float(rsi[-1]), 'adx': float(adx[-1]),
                'cmf': _cmf_val,
                'st_bull': st_bull, 'score': 10, 'pct_from_sup': 0.0,
                'mfi': float(mfi[-1]), 'macd_cross': False, 'stoch_cross': False,
            }, True
        return None, False

    # ═══════════════════════════════════════════
    #  SWING v3 — Gate likuiditas + Scoring 0-10
    # ═══════════════════════════════════════════
    # FIX: hapus gate c[-1]>=700 yang buang BBRI/BBCA/TLKM dll.
    # Ganti dengan filter likuiditas yang benar untuk IDX swing.
    if float(c[-1]) < 50: return None, False           # penny stock ekstrem
    if float(v[-1]) < 200_000: return None, False      # volume minimum 200K lot
    if not st_bull: return None, False
    if c[-1] <= e21[-1]: return None, False
    if vr < 1.3: return None, False
    if not (40 < rsi[-1] < 75): return None, False
    if mh[-1] <= 0: return None, False
    if adx[-1] <= 18: return None, False
    if c[-1] >= bb_mid[-1] + 1.5 * atr[-1]: return None, False

    score = 0.0
    if e9[-1] > e21[-1]: score += 1
    if n >= 55 and c[-1] > e50[-1]: score += 1
    if ml[-1] > ms[-1] and ml[-2] <= ms[-2]: score += 1
    elif ml[-1] > ms[-1]: score += 0.5
    if mh[-1] > mh[-2]: score += 1
    if 50 < rsi[-1] < 70: score += 1
    if sk[-1] > sd[-1] and sk[-2] <= sd[-2]: score += 1
    elif sk[-1] > sd[-1] and sk[-1] < 80: score += 0.5
    if vr >= 2.0: score += 1
    elif vr >= 1.5: score += 0.5
    if mfi[-1] > 55: score += 1
    if n >= 5 and obv[-1] > obv[-5]: score += 0.5
    if adx[-1] > 25: score += 1
    if dip[-1] > dim[-1]: score += 0.5
    if n >= 6 and c[-1] > c[-6]: score += 1
    if n >= 11 and c[-1] > float(np.max(c[-11:-1])): score += 1
    bw_arr = bb_bw[max(0, n-50):n]
    if len(bw_arr) >= 10 and bb_bw[-1] < float(np.nanpercentile(bw_arr, 25)): score += 0.5

    # ── v3.0 Bonus scoring dari indikator baru ────────────────
    try:
        _sqz_data = hitung_squeeze_momentum(df)
        if _sqz_data['sqz_off'] and _sqz_data['sqz_mom'] > 0:
            score += 1.5  # squeeze baru pecah ke atas = high-conviction
        elif _sqz_data['sqz_on'] and _sqz_data['sqz_mom'] > 0:
            score += 0.5  # masih squeeze tapi momentum positif
    except Exception:
        pass

    try:
        _div_data = detect_rsi_divergence(df)
        if _div_data['bull_div']:    score += 1.0  # regular bullish divergence
        if _div_data['hidden_bull']: score += 0.5  # hidden bullish (trend continuation)
    except Exception:
        pass

    try:
        _, _trail_trend, _trail_dist = hitung_atr_trailing_stop(df, mult=2.5, period=14)
        if _trail_trend[-1] == 1 and _trail_dist > 1.0:
            score += 0.5  # ATR trail konfirmasi uptrend + ruang gerak cukup
    except Exception:
        pass

    try:
        _ha_o2, _, _, _ha_c2, _ha_streak = hitung_heikin_ashi(df)
        if _ha_streak >= 3: score += 0.5  # 3+ candle HA hijau berturut = trend kuat
    except Exception:
        pass
    # ─────────────────────────────────────────────────────────
    score = min(score, 10)

    if score < 6: return None, False

    pct_from_sup = ((c[-1] - bb_bot[-1]) / (bb_bot[-1] + 1e-10)) * 100

    # Ambil nilai baru untuk output
    _sqz_flag = False; _div_flag = False; _ha_stk = 0; _sqz_off = False
    try:
        _sq = hitung_squeeze_momentum(df)
        _sqz_flag = bool(_sq['sqz_on']); _sqz_off = bool(_sq['sqz_off'])
    except Exception: pass
    try:
        _dv = detect_rsi_divergence(df)
        _div_flag = bool(_dv['bull_div'] or _dv['hidden_bull'])
    except Exception: pass
    try:
        _, _, _, _, _ha_stk = hitung_heikin_ashi(df)
    except Exception: pass

    return {
        'ticker':       ticker.upper(),
        'close':        float(c[-1]),
        'vol_ratio':    vr,
        'rsi':          float(rsi[-1]),
        'adx':          float(adx[-1]),
        'cmf':          _cmf_val,
        'st_bull':      st_bull,
        'score':        round(score, 1),
        'pct_from_sup': round(pct_from_sup, 1),
        'mfi':          float(mfi[-1]),
        'macd_cross':   bool(ml[-1] > ms[-1] and ml[-2] <= ms[-2]),
        'stoch_cross':  bool(sk[-1] > sd[-1] and sk[-2] <= sd[-2]),
        # v3.0 extras
        'sqz_on':    _sqz_flag,
        'sqz_off':   _sqz_off,
        'div_bull':  _div_flag,
        'ha_streak': int(_ha_stk),
    }, True

# ============================================================
#  CHART GENERATOR
# ============================================================
def buat_chart(hasil: dict) -> io.BytesIO:
    df = hasil['_df'].copy(); N = min(80, len(df)); df = df.tail(N).reset_index()
    up = hasil['_up'][-N:]; dn = hasil['_dn'][-N:]; tr = hasil['_trend'][-N:]
    bk = hasil['_buy_k'][-N:]; sk = hasil['_sell_k'][-N:]
    bb = hasil['_buy_b'][-N:]; sb = hasil['_sell_b'][-N:]
    atr = hasil['_atr_arr'][-N:]; e9 = hasil['_ema9'][-N:]; vw = hasil['_vwap'][-N:]
    st  = np.where(tr == 1, up, dn)

    BG = '#131722'; GR = '#1e222d'; TX = '#d1d4dc'
    MU = '#787b86'; GN = '#26a69a'; RD = '#ef5350'; YL = '#f9a825'

    # Ukuran lebih mobile-friendly: rasio ~1.78:1 (16:9-ish), pas di Telegram
    fig = plt.figure(figsize=(11, 6.5), facecolor=BG)

    # ── Background foto ─────────────────────────────────────────
    # Pakai imshow di full-figure axes supaya masuk bbox_inches='tight'
    # (fig.figimage tidak termasuk dalam tight bounding box → black padding)
    _has_bg = False
    if _CHART_BG is not None:
        try:
            _ax_bg = fig.add_axes([0, 0, 1, 1], zorder=0)
            _ax_bg.imshow(np.array(_CHART_BG), aspect='auto',
                          extent=[0, 1, 0, 1], alpha=0.35,
                          zorder=0, interpolation='bilinear')
            _ax_bg.set_xlim(0, 1); _ax_bg.set_ylim(0, 1)
            _ax_bg.axis('off')
            _has_bg = True
        except Exception:
            pass

    gs  = fig.add_gridspec(3, 1, height_ratios=[4, 0.18, 0.85], hspace=0.04)
    ax1  = fig.add_subplot(gs[0])
    axha = fig.add_subplot(gs[1], sharex=ax1)   # HA trend strip
    ax2  = fig.add_subplot(gs[2], sharex=ax1)   # volume
    for ax in (ax1, axha, ax2):
        # Transparan jika ada foto BG, solid jika tidak
        ax.set_facecolor((0, 0, 0, 0) if _has_bg else BG)
        ax.patch.set_alpha(0.0 if _has_bg else 1.0)
        for sp in ax.spines.values(): sp.set_color(GR)
        ax.tick_params(colors=MU, labelsize=7.5)
        ax.yaxis.set_label_position('right'); ax.yaxis.tick_right()
        ax.grid(axis='y', color=GR, linewidth=0.5, alpha=0.5)
        ax.grid(axis='x', color=GR, linewidth=0.3, alpha=0.4)

    xs = np.arange(N)
    op = df['Open'].values; hi = df['High'].values
    lo = df['Low'].values;  cl = df['Close'].values

    # ── v3.0: Heikin Ashi — body color dari HA, wick dari real OHLC ──────────
    # Pine equivalent: candle body warna = HA direction, tapi wick = real H/L
    # Keuntungan: trend lebih smooth, noise berkurang, tapi range nyata tetap terlihat
    try:
        _ha_o2, _ha_h2, _ha_l2, _ha_c2, _ha_stk2 = hitung_heikin_ashi(
            hasil['_df'].tail(N + 5).copy())
        _ha_c2 = _ha_c2[-N:]; _ha_o2 = _ha_o2[-N:]
        _use_ha = True
    except Exception:
        _ha_c2 = cl; _ha_o2 = op; _ha_stk2 = 0; _use_ha = False

    for i in xs:
        if _use_ha:
            # Body warna = HA direction (smoother trend signal)
            col      = GN if _ha_c2[i] >= _ha_o2[i] else RD
            col_wick = '#4caf50' if _ha_c2[i] >= _ha_o2[i] else '#ef5350'
        else:
            col = col_wick = GN if cl[i] >= op[i] else RD
        # Body: pakai real OHLC untuk akurasi harga
        ax1.bar(i, max(abs(cl[i] - op[i]), 0.1), bottom=min(op[i], cl[i]),
                color=col, width=0.6, zorder=3)
        # Wick: real OHLC H/L
        ax1.plot([i, i], [lo[i], hi[i]], color=col_wick, linewidth=0.9, zorder=2)

    # Annotasi HA streak di pojok chart
    if _use_ha and _ha_stk2 >= 3:
        ax1.annotate(f'HA🟢×{_ha_stk2}', xy=(0.01, 0.97),
                     xycoords='axes fraction',
                     color='#69f0ae', fontsize=7, va='top',
                     bbox=dict(boxstyle='round,pad=0.2', facecolor='#1a2a1a', alpha=0.7))
    elif _use_ha and _ha_stk2 <= -3:
        ax1.annotate(f'HA🔴×{abs(_ha_stk2)}', xy=(0.01, 0.97),
                     xycoords='axes fraction',
                     color='#ff5252', fontsize=7, va='top',
                     bbox=dict(boxstyle='round,pad=0.2', facecolor='#2a1a1a', alpha=0.7))
    # ─────────────────────────────────────────────────────────────────────────

    # ── HA Trend Strip (axha) ─────────────────────────────────────────────────
    # Panel tipis di bawah chart: tiap bar berwarna hijau/merah sesuai HA direction
    # Selalu visible, tidak perlu threshold streak
    axha.set_ylim(0, 1)
    axha.set_yticks([])
    axha.tick_params(bottom=False, labelbottom=False)
    for sp in axha.spines.values(): sp.set_visible(False)
    if _use_ha:
        for i in xs:
            ha_col = '#26a69a' if _ha_c2[i] >= _ha_o2[i] else '#ef5350'
            axha.bar(i, 1.0, bottom=0, color=ha_col, width=0.85, alpha=0.85)
        # Label kiri strip
        axha.text(-0.5, 0.5, 'HA', color=MU, fontsize=6.5, va='center',
                  ha='right', transform=axha.get_yaxis_transform())
        # Label streak di kanan strip
        streak_color = '#69f0ae' if _ha_stk2 > 0 else '#ff5252'
        streak_icon  = '🟢' if _ha_stk2 > 0 else '🔴'
        axha.text(N - 0.5, 0.5,
                  f' {streak_icon}×{abs(_ha_stk2)}',
                  color=streak_color, fontsize=6.5, va='center')
    # ─────────────────────────────────────────────────────────────────────────
    for i in range(1, N):
        ax1.plot([i - 1, i], [st[i - 1], st[i]],
                 color=GN if tr[i] == 1 else RD, linewidth=1.8, zorder=4)
    ax1.plot(xs, e9, color='#29b6f6', linewidth=1.2, label='EMA9', zorder=3)
    ax1.plot(xs, vw, color='#ab47bc', linewidth=1.5, linestyle='-.', label='VWAP', zorder=3)
    ax1.legend(loc='upper left', fontsize=7, facecolor=BG, edgecolor=GR, labelcolor=TX)
    if hasil['resistance']:
        ax1.axhline(hasil['resistance'], color=RD, linestyle='--', linewidth=1.1, alpha=0.85)
        ax1.axhspan(hasil['resistance'], hasil['resistance'] + hasil['atr'] * 0.3,
                    color=RD, alpha=0.13, zorder=1)
    if hasil['support']:
        ax1.axhline(hasil['support'], color=GN, linestyle='--', linewidth=1.1, alpha=0.85)
        ax1.axhspan(hasil['support'] - hasil['atr'] * 0.3, hasil['support'],
                    color=GN, alpha=0.13, zorder=1)

    # ── v3.0: Pivot Points harian ─────────────────────────────
    try:
        _pvt_df = hasil.get('_df')
        if _pvt_df is not None:
            _pvt = hitung_pivot_points(_pvt_df)
            _pp_val = _pvt['pp']
            _r1_val = _pvt['r1']; _s1_val = _pvt['s1']
            _r2_val = _pvt['r2']; _s2_val = _pvt['s2']
            # PP line
            ax1.axhline(_pp_val, color='#f9a825', linestyle=':', linewidth=1.0, alpha=0.7)
            ax1.text(N - 0.5, _pp_val, ' PP', color='#f9a825', fontsize=6, va='center', alpha=0.85)
            # R1/S1
            ax1.axhline(_r1_val, color='#ef9a9a', linestyle=':', linewidth=0.9, alpha=0.65)
            ax1.text(N - 0.5, _r1_val, ' R1', color='#ef9a9a', fontsize=6, va='center', alpha=0.8)
            ax1.axhline(_s1_val, color='#a5d6a7', linestyle=':', linewidth=0.9, alpha=0.65)
            ax1.text(N - 0.5, _s1_val, ' S1', color='#a5d6a7', fontsize=6, va='center', alpha=0.8)
            # R2/S2 (lebih tipis)
            ax1.axhline(_r2_val, color='#ef9a9a', linestyle=':', linewidth=0.7, alpha=0.4)
            ax1.axhline(_s2_val, color='#a5d6a7', linestyle=':', linewidth=0.7, alpha=0.4)
    except Exception:
        pass
    # ─────────────────────────────────────────────────────────
    for i in xs:
        if bk[i]:
            ax1.annotate('🚀 BELI', xy=(i, lo[i] - atr[i] * 0.3),
                         color='#ffffff', fontsize=8.5, fontweight='bold',
                         ha='center', va='top',
                         bbox=dict(boxstyle='round,pad=0.25', facecolor='#1b5e20', alpha=0.85, edgecolor='#69f0ae', linewidth=0.8))
        elif sk[i]:
            ax1.annotate('🔻 JUAL', xy=(i, hi[i] + atr[i] * 0.3),
                         color='#ffffff', fontsize=8.5, fontweight='bold',
                         ha='center', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.25', facecolor='#7f0000', alpha=0.85, edgecolor='#ff5252', linewidth=0.8))
        elif bb[i]:
            ax1.annotate('beli?', xy=(i, lo[i] - atr[i] * 0.3),
                         color='#e0f2f1', fontsize=7.5, ha='center', va='top',
                         bbox=dict(boxstyle='round,pad=0.18', facecolor='#1a3a2a', alpha=0.75, edgecolor='none'))
        elif sb[i]:
            ax1.annotate('jual?', xy=(i, hi[i] + atr[i] * 0.3),
                         color='#fce4ec', fontsize=7.5, ha='center', va='bottom',
                         bbox=dict(boxstyle='round,pad=0.18', facecolor='#3a1a1a', alpha=0.75, edgecolor='none'))

    ax2.bar(xs, df['Volume'].values.astype(float),
            color=[GN if cl[i] >= op[i] else RD for i in xs], alpha=0.75, width=0.6)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda x, _: f'{x / 1e6:.1f}M' if x >= 1e6 else f'{x / 1e3:.0f}K'))

    ic = 'Datetime' if 'Datetime' in df.columns else df.columns[0]
    dt = pd.to_datetime(df[ic])
    if dt.dt.tz is None: dt = dt.dt.tz_localize('UTC')
    dt = dt.dt.tz_convert('Asia/Jakarta')
    tk = list(range(0, N, max(1, N // 8))); ax2.set_xticks(tk)
    ax2.set_xticklabels([dt.iloc[i].strftime('%d %b %H:%M') for i in tk],
                         color=MU, fontsize=7.5)
    plt.setp(ax1.get_xticklabels(), visible=False)
    plt.setp(axha.get_xticklabels(), visible=False)

    tl = 'UPTREND' if hasil['trend'] == 1 else 'DOWNTREND'
    ax1.set_title(f"{hasil['ticker']}  |  {hasil['tf'].upper()}  |  {tl}",
                  color=TX, fontsize=11, fontweight='bold', pad=7, loc='left')
    fig.text(0.98, 0.01, datetime.now(WIB).strftime('%H:%M:%S WIB %d/%m/%Y'),
             color=MU, fontsize=7, ha='right', va='bottom')
    # Watermark
    ax1.text(0.995, 0.012, '@bot_radar', transform=ax1.transAxes,
             ha='right', va='bottom', fontsize=6.5, color=TX, alpha=0.12, style='italic')
    plt.subplots_adjust(left=0.05, right=0.87, top=0.93, bottom=0.08)
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=130, bbox_inches=None,
                facecolor=fig.get_facecolor(), edgecolor='none')
    plt.close(fig); buf.seek(0)
    return buf

def format_single(r: dict) -> str:
    ic   = "🟢 UPTREND" if r['trend'] == 1 else "🔴 DOWNTREND"
    rs   = f"{r['resistance']:,.0f}" if r['resistance'] else "N/A"
    sp   = f"{r['support']:,.0f}"    if r['support']    else "N/A"
    # Jarak harga ke support/resistance
    dist_sup = ((r['close'] - r['support'])   / r['support']   * 100) if r.get('support')    else None
    dist_res = ((r['resistance'] - r['close']) / r['close']     * 100) if r.get('resistance') else None
    dist_s   = f"+{dist_sup:.1f}%" if dist_sup is not None else ""
    dist_r   = f"-{dist_res:.1f}%" if dist_res is not None else ""

    rsi_v  = r.get('rsi',   0.0)
    adx_v  = r.get('adx',   0.0)
    mfi_v  = r.get('mfi',   0.0)
    vwap_v = r.get('vwap',  0.0)
    e9_v   = r.get('ema9',  0.0)
    e21_v  = r.get('ema21', 0.0)
    mh_v   = r.get('macd_hist', 0.0)

    # RSI zone
    rsi_tag = "OB" if rsi_v > 70 else ("OS" if rsi_v < 30 else "OK")
    # Sinyal indikator
    sig_ema  = "✅" if e9_v > e21_v else "❌"
    sig_macd = "✅" if mh_v > 0    else "❌"
    sig_mfi  = "✅" if mfi_v > 50  else "❌"
    sig_vwap = "✅" if r['close'] > vwap_v else "❌"

    lines = [
        f"*[{r['ticker']}] SnR+ST+VWAP | {r['tf'].upper()}*", "```",
        f"Harga      : {r['close']:>12,.0f}",
        f"SuperTrend : {r['st_line']:>12,.0f}  {ic}",
        f"Resistance : {rs:>12}  {dist_r}",
        f"Support    : {sp:>12}  {dist_s}",
        f"VWAP       : {vwap_v:>12,.0f}  {sig_vwap}",
        f"EMA 9>21   : {'Ya' if e9_v > e21_v else 'Tidak':>12}  {sig_ema}",
        f"MACD Hist  : {mh_v:>12.2f}  {sig_macd}",
        f"RSI(14)    : {rsi_v:>12.1f}  [{rsi_tag}]",
        f"ADX(14)    : {adx_v:>12.1f}",
        f"MFI(14)    : {mfi_v:>12.1f}  {sig_mfi}",
        f"Volume     : {'Cukup ✅' if r['vol_cukup'] else 'Rendah ❌':>12}", "```",
    ]
    if r['buy_kuat']:    lines.append("*[🚀 BELI KUAT]* ST naik + dekat Demand + Vol tinggi")
    elif r['sell_kuat']: lines.append("*[🔻 JUAL KUAT]* ST turun + dekat Supply + Vol tinggi")
    elif r['trend'] == 1 and rsi_v < 60 and mh_v > 0:
        lines.append("_💡 Setup menarik — pantau breakout_")
    return "\n".join(lines)

# ============================================================
#  IHSG
# ============================================================
async def fetch_ihsg() -> dict | None:
    loop = asyncio.get_event_loop()
    def _do():
        import warnings
        def _dl_info():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with contextlib.redirect_stdout(_io_sup.StringIO()), \
                     contextlib.redirect_stderr(_io_sup.StringIO()):
                    return yf.Ticker('^JKSE').info or {}

        def _dl_hist():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with contextlib.redirect_stdout(_io_sup.StringIO()), \
                     contextlib.redirect_stderr(_io_sup.StringIO()):
                    return yf.Ticker('^JKSE').history(period='6d', interval='1d')

        _dl_info.__name__ = 'yf_ihsg_info'
        _dl_hist.__name__ = 'yf_ihsg_hist'
        try:
            info = yf_safe_call(_dl_info, ttl=60)
            hist = yf_safe_call(_dl_hist, ttl=60)
            return info, hist
        except Exception:
            return None, None

    return await loop.run_in_executor(_IO_POOL, _do)

# ============================================================
#  MONEY FLOW (simplified, 1m candles)
# ============================================================
async def fetch_mf(ticker: str) -> dict | None:
    loop = asyncio.get_event_loop()

    def _do():
        import warnings
        def _dl():
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                with contextlib.redirect_stdout(_io_sup.StringIO()), \
                     contextlib.redirect_stderr(_io_sup.StringIO()):
                    df = yf.download(kode_yf(ticker), interval='1m', period='1d',
                                     progress=False, auto_adjust=True)
            if df is None or df.empty or len(df) < 10:
                return None
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            return df.dropna()
        _dl.__name__ = f'yf_mf1m_{ticker}'
        return yf_safe_call(_dl, ttl=60)

    df = await loop.run_in_executor(_IO_POOL, _do)
    if df is None: return None

    close = df['Close'].values
    open_ = df['Open'].values
    vol   = df['Volume'].values

    buy_vol  = float(vol[close >= open_].sum())
    sell_vol = float(vol[close < open_].sum())
    total    = buy_vol + sell_vol
    if total == 0: return None

    buy_pct  = buy_vol / total * 100
    sell_pct = sell_vol / total * 100
    net      = buy_vol - sell_vol
    last     = float(close[-1])
    first    = float(close[0])
    chg_pct  = (last - first) / first * 100 if first else 0

    # Net buy > 55% → AKUMULASI, < 45% → DISTRIBUSI
    if   buy_pct > 55: verdict = '🟢 AKUMULASI'
    elif sell_pct > 55: verdict = '🔴 DISTRIBUSI'
    else:               verdict = '🟡 NETRAL'

    result = {
        'ticker': ticker.upper(),
        'last': last, 'chg_pct': chg_pct,
        'buy_vol': buy_vol, 'sell_vol': sell_vol,
        'buy_pct': buy_pct, 'sell_pct': sell_pct,
        'net': net, 'verdict': verdict,
        'candles': len(df),
    }
    return result

# ============================================================
#  ALGO AUTO-SCAN
# ============================================================
def _kirim_algo_dm(text: str, chat_id: int) -> bool:
    """Kirim sinyal algo via ALGO_BOT_TOKEN (DM langsung ke owner algo).
    Fallback ke False kalau token tidak di-set."""
    if not ALGO_BOT_TOKEN:
        return False
    import requests as _req
    try:
        r = _req.post(
            f"https://api.telegram.org/bot{ALGO_BOT_TOKEN}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10
        )
        return r.json().get("ok", False)
    except Exception as _e:
        logger.warning("_kirim_algo_dm failed: %s", _e)
        return False


async def prewarm_all_tickers(app=None):
    """
    Pre-download semua IDX_STOCKS sebelum market open (jadwal 08:55 WIB).
    Setelah selesai, /swing /scr /algo TIDAK perlu download lagi → langsung dari cache.

    Estimasi waktu pada Termux 5G dengan n_parallel=6:
        957 ticker / 50 per-batch = 20 batch → 4 grup × ~3s = 12–20 detik.
    """
    t0 = time.time()
    logger.info("⏳ Pre-warm 08:55: mulai download %d ticker daily...", len(IDX_STOCKS))
    batches = [IDX_STOCKS[i:i + SCAN_BATCH] for i in range(0, len(IDX_STOCKS), SCAN_BATCH)]
    np_val  = int(os.getenv("N_PARALLEL", 6))
    all_dfs = await prefetch_parallel(batches, '1d', '6mo', n_parallel=np_val)
    elapsed = time.time() - t0
    hit_pct = len(all_dfs) / len(IDX_STOCKS) * 100
    logger.info("✅ Pre-warm selesai: %d/%d ticker (%.0f%%) cached dalam %.1fs",
                len(all_dfs), len(IDX_STOCKS), hit_pct, elapsed)


async def jalankan_eod_screen(app=None):
    """
    EOD Screen (15:45 WIB setiap hari kerja):
    Score semua IDX_STOCKS, simpan top 75 ke tabel gainer_candidates.
    Morning trigger 09:05 hanya download 50 dari sini → jauh lebih cepat.
    """
    logger.info("📊 EOD Screen: scoring %d saham...", len(IDX_STOCKS))
    t0 = time.time()
    np = int(os.getenv("N_PARALLEL", 6))

    batches = [IDX_STOCKS[i:i + SCAN_BATCH] for i in range(0, len(IDX_STOCKS), SCAN_BATCH)]
    all_dfs = await prefetch_parallel(batches, '1d', '6mo', n_parallel=np)

    loop = asyncio.get_event_loop()

    def _score_all():
        out = []
        for ticker, df in all_dfs.items():
            ctx = build_context(df, ticker=ticker)
            if ctx is None: continue

            # ── Hard gates — WAJIB lolos sebelum scoring ──────────────────────
            vr  = float(ctx.get('vol_ratio', 0))
            vol = float(ctx.get('volume', 0))
            rsi = float(ctx.get('rsi', 50))
            # 1. Volume minimum: VR ≥ 1.0x (setidaknya rata-rata)
            #    Tujuan: buang ALDO (0.1x), BKDP (0.0x), AMIN (0.4x)
            if vr < 1.0: continue
            # 2. Lot minimum: 100K lot (Rp10M untuk saham Rp100)
            #    Tujuan: buang saham susah dieksekusi
            if vol < 100_000: continue
            # 3. RSI tidak overbought — parabolic sudah lewat puncak
            if rsi > 80: continue
            # 4. SuperTrend harus bullish — tidak mau counter-trend
            if ctx.get('st_trend', 0) != 1: continue
            # ─────────────────────────────────────────────────────────────────

            sc = hitung_topgainer_score(ctx)
            if sc < 8: continue
            out.append({
                'ticker':    ticker,
                'score':     sc,
                'close':     ctx['close'],
                'vol_ratio': vr,
                'rsi':       rsi,
                'cmf':       ctx['cmf'],
                'scl':       int(ctx.get('scl', 0)),
                'swl':       int(ctx.get('swl', 0)),
                'sektor':    _SEKTOR_MAP.get(ticker, ''),
            })
        out.sort(key=lambda x: x['score'], reverse=True)
        return out[:75]

    scored = await loop.run_in_executor(_IO_POOL, _score_all)

    today = datetime.now(WIB).strftime('%Y-%m-%d')
    conn  = db_conn()
    conn.execute("DELETE FROM gainer_candidates WHERE scan_date != ?", (today,))
    for r in scored:
        conn.execute('''
            INSERT OR REPLACE INTO gainer_candidates
            (ticker,score,close,vol_ratio,rsi,cmf,scl,swl,sektor,scan_date,am_fired)
            VALUES (?,?,?,?,?,?,?,?,?,?,0)
        ''', (r['ticker'], r['score'], r['close'], r['vol_ratio'],
              r['rsi'],    r['cmf'],   r['scl'],   r['swl'],
              r['sektor'], today))
    conn.commit(); conn.close()

    elapsed = time.time() - t0
    top5 = ', '.join(f"{r['ticker']}({r['score']})" for r in scored[:5])
    logger.info("✅ EOD Screen: %d kandidat, %.1fs. Top5: %s", len(scored), elapsed, top5)

    # Kirim summary ke semua admin
    if app and scored:
        lines = [f"📊 *EOD Top Gainer Candidates* — {today}\n"]
        for r in scored[:15]:
            icon = "⭐" if r['score'] >= 13 else ("🔸" if r['score'] >= 11 else "·")
            sk   = f" [{r['sektor'].upper()}]" if r['sektor'] else ""
            lines.append(
                f"{icon} `{r['ticker']:<6}` {r['score']:>2}/17  "
                f"Rp{r['close']:>7,.0f}  VR{r['vol_ratio']:.1f}x  "
                f"RSI{r['rsi']:.0f}  CMF{r['cmf']:+.2f}{sk}"
            )
        if len(scored) > 15:
            lines.append(f"_...+{len(scored)-15} lainnya. /topcan untuk lengkapnya._")
        msg = '\n'.join(lines)
        for aid in ADMIN_IDS:
            try: await app.bot.send_message(aid, msg, parse_mode='Markdown')
            except Exception: pass


async def jalankan_morning_trigger(app):
    """
    Morning Trigger (09:05 WIB):
    Download 5m data TOP 50 dari EOD kemarin.
    Cek gap-up + volume anomali → fire Telegram alert.
    Deteksi sektor cluster: jika 2+ saham sektor sama naik → SECTOR ALERT.
    """
    conn = db_conn()
    rows = conn.execute(
        "SELECT ticker, score, sektor FROM gainer_candidates "
        "WHERE am_fired=0 ORDER BY score DESC LIMIT 50"
    ).fetchall()
    conn.close()

    if not rows:
        logger.info("Morning trigger: kosong. Jalankan /topcan now dulu.")
        return

    tickers   = [r[0] for r in rows]
    score_map = {r[0]: r[1] for r in rows}
    sekt_map  = {r[0]: r[2] for r in rows}

    logger.info("🌅 Morning trigger: cek %d kandidat 5m...", len(tickers))
    batches = [tickers[i:i+20] for i in range(0, len(tickers), 20)]
    all_dfs = await prefetch_parallel(batches, '5m', '5d',
                                       n_parallel=int(os.getenv("N_PARALLEL", 6)))

    today_date = datetime.now(WIB).date()
    alerts: list[dict] = []

    for ticker in tickers:
        df = all_dfs.get(ticker)
        if df is None or len(df) < 10: continue

        # Normalize timezone → Asia/Jakarta
        df.index = pd.to_datetime(df.index)
        try:
            if df.index.tz is not None:
                df.index = df.index.tz_convert('Asia/Jakarta')
            else:
                df.index = df.index.tz_localize(
                    'Asia/Jakarta', ambiguous='infer', nonexistent='shift_forward')
        except Exception:
            pass

        t_mask = pd.to_datetime(df.index).normalize().dt.date == today_date
        today_df = df[t_mask]; prev_df = df[~t_mask]

        if len(today_df) < 1 or len(prev_df) < 5: continue

        prev_close  = float(prev_df['Close'].iloc[-1])
        open_today  = float(today_df['Open'].iloc[0])
        close_today = float(today_df['Close'].iloc[-1])
        vol_today   = float(today_df['Volume'].sum())

        avg_5m = float(prev_df['Volume'].mean())
        if avg_5m <= 0: continue

        vr_today = vol_today / (avg_5m * max(len(today_df), 1))
        gap_pct  = (open_today - prev_close) / prev_close * 100 if prev_close > 0 else 0
        mom_pct  = (close_today - open_today) / open_today * 100 if open_today > 0 else 0

        if gap_pct >= 2.0 and vr_today >= 2.0 and mom_pct >= -0.5:
            alerts.append({
                'ticker': ticker, 'gap': gap_pct, 'vr': vr_today,
                'mom': mom_pct,   'score': score_map[ticker],
                'sektor': sekt_map.get(ticker, ''),
                'close': close_today,
            })

    alerts.sort(key=lambda x: x['score'] * x['vr'], reverse=True)

    # Update am_fired
    if alerts:
        conn = db_conn()
        for a in alerts:
            conn.execute("UPDATE gainer_candidates SET am_fired=1 WHERE ticker=?",
                         (a['ticker'],))
        conn.commit(); conn.close()

    # Deteksi sektor cluster
    sekt_bucket: dict[str, list] = {}
    for a in alerts:
        sk = a['sektor']
        if sk: sekt_bucket.setdefault(sk, []).append(a['ticker'])

    now_str = datetime.now(WIB).strftime('%H:%M WIB')
    lines   = [f"🌅 *MORNING TRIGGER* — {now_str}\n"]

    for sk, members in sekt_bucket.items():
        if len(members) >= 2:
            lines.append(f"🔥 *SECTOR {sk.upper()}*: {' '.join(members)}")
    if any(len(v) >= 2 for v in sekt_bucket.values()):
        lines.append("")

    for a in alerts[:15]:
        sk_tag  = f"[{a['sektor'].upper()}] " if a['sektor'] else ""
        up_icon = "↑" if a['mom'] >= 0 else "↓"
        lines.append(
            f"🚀 *{a['ticker']}* {sk_tag}| "
            f"Gap +{a['gap']:.1f}% | VR {a['vr']:.1f}x | "
            f"Mom {up_icon}{abs(a['mom']):.1f}% | Scr {a['score']}/17"
        )

    if not alerts:
        lines.append("_Tidak ada kandidat gap≥2% + VR≥2x pagi ini._")

    msg     = '\n'.join(lines)
    chat_id = ALLOWED_CHAT_IDS[0] if ALLOWED_CHAT_IDS else None

    if chat_id and app:
        try:
            await app.bot.send_message(
                chat_id=chat_id, text=msg,
                parse_mode='Markdown',
                message_thread_id=ALGO_TOPIC_ID
            )
        except Exception as e:
            logger.error("Morning trigger send error: %s", e)
        # DM ke admin juga
        for aid in ADMIN_IDS:
            try: await app.bot.send_message(aid, msg, parse_mode='Markdown')
            except Exception: pass

    logger.info("🌅 Morning trigger: %d alerts, %d sector cluster(s)",
                len(alerts), sum(1 for v in sekt_bucket.values() if len(v) >= 2))


async def jalankan_algo_scan(app):
    if not is_market_open(): return
    conn  = db_conn()
    algos = conn.execute(
        "SELECT id,user_id,chat_id,formula,title FROM algos WHERE active=1"
    ).fetchall()
    conn.close()
    if not algos: return

    today   = datetime.now(WIB).strftime('%Y-%m-%d')
    now_str = datetime.now(WIB).strftime('%H:%M WIB')
    results: dict[int, list] = {a[0]: [] for a in algos}

    batches = [IDX_STOCKS[i:i + SCAN_BATCH] for i in range(0, len(IDX_STOCKS), SCAN_BATCH)]

    # Prefetch semua ticker paralel sebelum eval
    all_dfs = await prefetch_parallel(batches, '1d', '6mo', n_parallel=int(os.getenv("N_PARALLEL", 6)))

    # Baca fired list sekali saja (batch query, bukan per-ticker)
    conn_fired = db_conn()
    fired_set: set = set()
    for row in conn_fired.execute(
            "SELECT algo_id, ticker FROM algo_fired WHERE fired_date=?", (today,)).fetchall():
        fired_set.add((row[0], row[1]))
    conn_fired.close()

    for ticker, df in all_dfs.items():
        if len(df) < 40: continue
        ctx = build_context(df, ticker=ticker)
        if ctx is None: continue
        for algo_id, _, _, formula, _ in algos:
            if (algo_id, ticker) in fired_set: continue
            if eval_formula(formula, ctx):
                results[algo_id].append({
                    'ticker': ticker, 'close': ctx['close'],
                    'vol_ratio': ctx['vol_ratio'], 'rsi': ctx['rsi'],
                    'macd_hist': ctx['macd_hist'],
                })

    for algo_id, user_id, chat_id, formula, title in algos:
        found = results[algo_id]
        if not found: continue

        # ── Re-fetch harga live untuk ticker yang lolos filter ────────────
        # FIX v3.0: gunakan yf_safe_call agar lewat rate limiter + cache.
        live_prices: dict[str, float] = {}
        try:
            live_tickers = [r['ticker'] + '.JK' for r in found]

            def _live_dl_1m():
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    with contextlib.redirect_stdout(_io_sup.StringIO()), \
                         contextlib.redirect_stderr(_io_sup.StringIO()):
                        return yf.download(
                            live_tickers, period='5d', interval='1m',
                            progress=False, auto_adjust=True,
                            group_by='ticker', threads=True)
            _live_dl_1m.__name__ = f'yf_live1m_{algo_id}'
            _lv = yf_safe_call(_live_dl_1m, ttl=60)

            if _lv is not None:
                for r in found:
                    yt = r['ticker'] + '.JK'
                    try:
                        _lp = float(_lv['Close'].dropna().iloc[-1]) if len(found) == 1 \
                              else float(_lv[yt]['Close'].dropna().iloc[-1])
                        if _lp > 0: live_prices[r['ticker']] = _lp
                    except Exception:
                        pass
        except Exception as _lve:
            logger.warning("live price re-fetch 1m failed: %s", _lve)
            try:
                def _live_dl_5m():
                    import warnings
                    with warnings.catch_warnings():
                        warnings.simplefilter('ignore')
                        with contextlib.redirect_stdout(_io_sup.StringIO()), \
                             contextlib.redirect_stderr(_io_sup.StringIO()):
                            return yf.download(
                                [r['ticker'] + '.JK' for r in found],
                                period='5d', interval='5m',
                                progress=False, auto_adjust=True,
                                group_by='ticker', threads=True)
                _live_dl_5m.__name__ = f'yf_live5m_{algo_id}'
                _lv2 = yf_safe_call(_live_dl_5m, ttl=60)
                if _lv2 is not None:
                    for r in found:
                        yt = r['ticker'] + '.JK'
                        try:
                            _lp2 = float(_lv2['Close'].dropna().iloc[-1]) if len(found) == 1 \
                                   else float(_lv2[yt]['Close'].dropna().iloc[-1])
                            if _lp2 > 0: live_prices[r['ticker']] = _lp2
                        except Exception:
                            pass
            except Exception as _lve2:
                logger.warning("live price re-fetch 5m also failed: %s", _lve2)
        # ─────────────────────────────────────────────────────────────────

        conn3 = db_conn()
        for r in found:
            try:
                conn3.execute(
                    "INSERT OR IGNORE INTO algo_fired(algo_id,ticker,fired_date) VALUES(?,?,?)",
                    (algo_id, r['ticker'], today))
            except Exception:
                pass
        conn3.commit(); conn3.close()

        fl    = formula if len(formula) <= 70 else formula[:67] + '...'
        lines = [f"⚡ *ALGO [{algo_id}]: {title}* `{now_str}`", f"`{fl}`", "```",
                 f"{'Ticker':<6}  {'Harga':>8}  {'VR':>5}  {'RSI':>5}  {'MACD H':>7}"]
        for r in sorted(found, key=lambda x: x['vol_ratio'], reverse=True):
            mh = f"+{r['macd_hist']:.1f}" if r['macd_hist'] >= 0 else f"{r['macd_hist']:.1f}"
            harga_tampil = live_prices.get(r['ticker'], r['close'])  # live jika ada
            lines.append(
                f"{r['ticker']:<6}  {harga_tampil:>8,.0f}  "
                f"{r['vol_ratio']:>4.1f}×  {r['rsi']:>5.1f}  {mh:>7}")
        lines.append("```")
        full_text = '\n'.join(lines)

        # ── Routing sinyal: DM ke owner SELALU, tambahan ke grup kalau diset ──
        dm_target = user_id  # selalu DM ke owner algo
        sent = False
        if ALGO_BOT_TOKEN:
            sent = _kirim_algo_dm(full_text, dm_target)
            if sent:
                logger.info("Algo '%s' → DM uid=%s via ALGO_BOT", title, dm_target)

        if not sent:
            # 1. Selalu kirim DM ke owner algo
            try:
                await app.bot.send_message(
                    chat_id=dm_target, text=full_text, parse_mode='Markdown')
                logger.info("Algo '%s' → DM uid=%s via main bot", title, dm_target)
            except Exception as _e1:
                try:
                    await app.bot.send_message(
                        chat_id=dm_target, text=full_text[:3800], parse_mode='Markdown')
                except Exception as _e2:
                    logger.warning("algo DM owner uid=%s failed: %s", dm_target, _e2)

            # 2. JUGA kirim ke grup/topic kalau ALGO_CHAT_ID diset
            if ALGO_CHAT_ID:
                _gkw = {'chat_id': ALGO_CHAT_ID, 'text': full_text, 'parse_mode': 'Markdown'}
                if ALGO_TOPIC_ID:
                    _gkw['message_thread_id'] = ALGO_TOPIC_ID
                try:
                    await app.bot.send_message(**_gkw)
                except Exception:
                    try:
                        _gkw['text'] = full_text[:3800]
                        await app.bot.send_message(**_gkw)
                    except Exception as _eg:
                        logger.warning("algo group send failed: %s", _eg)

# Evict cache tiap 10 menit (sudah dihandle _cache_janitor thread, ini compat untuk scheduler)
async def _cache_evict_job():
    _yf_cache_clear_expired()
    logger.info("YF cache evict (scheduler). Entries: %d", len(_YF_CACHE))

# ============================================================
#  TEKS PANDUAN
# ============================================================
TEKS_FORMULA = """
*📐 /formula — Variabel & Fungsi Bot v3.0*
_Referensi: DSI Style (datasaham.id)_

*─ OHLCV (candle saat ini) ─*
`close` `open` `high` `low` `volume` `mid_price`

*─ Candle sebelumnya ─*
`prev_close` `prev_open` `prev_high` `prev_low` `prev_volume`
`prev2_close` `prev2_open` `prev2_high` `prev2_low`
`prev3_close` `prev3_high` `prev3_low`
Fungsi: `prev_c(N)` `prev_h(N)` `prev_l(N)` → candle N bar lalu

*─ Moving Average ─*
`sma5`  `sma20`  `sma26`  `sma200`
`ema9`  `ema21`  `ema50`
`prev_sma5`  `prev_sma20`
`prev_ema9`  `prev_ema21`  `prev_ema50`
Fungsi: `sma(N)` `ema(N)` → period custom

*─ MACD(12,26,9) ─*
`macd` `macd_signal` `macd_hist`
`prev_macd` `prev_macd_signal`
`prev_macd_hist` `prev2_macd_hist`

*─ RSI(14) ─*
`rsi` `prev_rsi`
Fungsi: `rsi_n(N)` → RSI period custom

*─ Stochastic(15,3,3) ─*
`stoch_k` `stoch_d` `prev_stoch_k` `prev_stoch_d`

*─ ADX(14) ─*
`adx` `di_plus` `di_minus`

*─ Bollinger Bands(20,2) ─*
`bb_top` `bb_bottom` `bb_mid`
`bb_bw` → bandwidth · `bb_pct` → %B
`prev_bb_top` `prev_bb_bottom`

*─ Indikator Lain ─*
`atr` `prev_atr` · Fungsi: `atr_n(N)`
`cci` `prev_cci` · Fungsi: `cci_n(N)`
`mfi` `prev_mfi` · Fungsi: `mfi_n(N)`
`williams_r` `prev_williams_r`
`roc` `prev_roc` · Fungsi: `roc_n(N)`
`obv` `prev_obv` · `sar` · `vwap` `prev_vwap`

*─ Volume ─*
`vol_avg` → SMA20 volume · `vol_ratio` → volume/vol\\_avg

*─ Fungsi Range ─*
`hhv(N)` → highest high N bar
`llv(N)` → lowest low N bar
`hhv_close(N)` `llv_close(N)` → range pada close
`vol_sma(N)` → SMA volume N bar

*─ 🆕 v3.0: Pivot Points ─*
`pp` `r1` `r2` `r3` → PP + Resistance 1/2/3
`s1` `s2` `s3` → Support 1/2/3
`cr4` `cr3` `cs3` `cs4` → Camarilla R/S

*─ 🆕 v3.0: Heikin Ashi ─*
`ha_close` `ha_open` · `ha_bull_streak` → HA hijau berturut (>=3 = trend kuat)

*─ 🆕 v3.0: Squeeze Momentum ─*
`sqz_on` → BB di dalam KC (konsolidasi)
`sqz_off` → bar pertama breakout dari squeeze
`sqz_mom` → arah momentum (>0 = up)
`no_sqz` → tidak dalam squeeze

*─ 🆕 v3.0: ATR Trailing Stop ─*
`trail_stop` → nilai trail stop (2.5x ATR14)
`trail_dist_pct` → jarak close ke trail (%)

*─ 🆕 v3.0: Volume Profile ─*
`poc` → Point of Control (volume terbesar)
`va_high` `va_low` → Value Area High/Low (70% vol)

*─ 🆕 v3.0: RSI Divergence ─*
`bull_div` → regular bullish divergence
`bear_div` → regular bearish divergence
`hidden_bull` → hidden bullish (trend continuation)

*─ Operator ─*
`>` `<` `>=` `<=` `==` `!=` `and` `or` `not`

_Pisah kondisi di /scr dan /algo dengan *+* bukan *and*_

*─ 🆕 v3.1: CMF (Chaikin Money Flow) ─*
`cmf` → nilai CMF 20 bar (−1.0 … +1.0)
`prev_cmf` → CMF bar sebelumnya
`cmf_bull` → cmf > 0.05 (tekanan beli)
`cmf_bear` → cmf < −0.05 (tekanan jual)

*─ 🆕 v3.1: Proximity S/D Zone ─*
`near_demand` → harga dekat support/demand zone (±0.5 ATR)
`near_supply` → harga dekat resistance/supply zone (±0.5 ATR)
`vol_spike` → volume ≥ 1.5× MA20

*─ 🆕 v3.1: HTF (Daily Regime) ─*
`htf_bull` → Daily SuperTrend bullish + harga > EMA50
`htf_bear` → Daily SuperTrend bearish + harga < EMA50
`htf_rsi` → RSI Daily (14)
`htf_cmf` → CMF Daily (20)

*─ 🆕 v3.1: Confluence Scores ─*
`scl` → Scalp Long score 0–9 (≥6 = sinyal kuat)
`scs` → Scalp Short score 0–9
`swl` → Swing Long score 0–10 (≥7 = sinyal kuat)
`sws` → Swing Short score 0–10

_Contoh formula pakai skor:_
`/algo (scl >= 6 + htf_bull + cmf > 0.05) Scalp Long MTF`
`/algo (swl >= 7 + cmf_bull + near_demand) Swing Long HTF`
`/scr (cmf > 0.15 + vol_spike + rsi > 50 + htf_bull) CMF Breakout`

*─ 🆕 v3.2: Hetrik 7-Candle Cycle ─*
`hetrik` → True jika pola 3+ candle berurutan terdeteksi
`hetrik_bull` → streak candle hijau (bullish)
`hetrik_bear` → streak candle merah (bearish)
`hetrik_streak` → panjang streak (3–10)
`hetrik_phase` → posisi cycle: 3=C3 entry, 4=C4 exhaustion, 5=C5 retrace, 6–7=push
`hetrik_score` → skor 0–6 (≥3 = layak perhatian, ≥4 = kuat)
`hetrik_grg` → Green→Red→Green pattern (Egy Setiawan) / Red→Green→Red (short)
`hetrik_c4_exh` → C4 exhaustion: volume↓ + dominant wick > 35%
`hetrik_c4_wick` → C4 wick ratio (0–1)
`hetrik_c5_retrace` → C5 retracement terkonfirmasi
`hetrik_entry_ref` → Close C3 (level re-entry setelah retrace)
`hetrik_vol_ratio` → volume C3 / vol avg20 (≥1.5 = breakout kuat)

_Contoh formula hetrik:_
`/algo (hetrik_bull + hetrik_phase == 5 + htf_bull) Hetrik Retrace Entry`
`/algo (hetrik_grg + hetrik_score >= 4 + vol_ratio >= 1.5) GRG Bull Entry`
`/scr (hetrik + hetrik_score >= 4 + hetrik_bull + rsi > 50) Hetrik Bull Scan`
"""

TEKS_CONTOH = """
*📋 CONTOH FORMULA SIAP PAKAI*

*─ Pola Candle ─*
Higher High Higher Low:
`high > prev_high and low > prev_low`

3 White Soldiers:
`prev2_close > prev2_open and prev_close > prev_open and close > open`

*Bullish Engulfing* 🕯️
`prev_close < prev_open and close > open and open <= prev_close and close >= prev_open and vol_ratio >= 1.5`
_(bearish → bullish, badan candle sekarang menelan candle sebelumnya, volume naik)_

*─ Breakout ─*
`close > hhv(20) and vol_ratio >= 2 and rsi > 50`
`close > bb_top and vol_ratio >= 2 and rsi < 75`

*─ Buy on Weakness ─*
`rsi < 30 and stoch_k < 20 and stoch_k > stoch_d`
`williams_r < -80 and mfi < 25 and close > prev_close`

*─ Golden Cross ─*
`prev_ema9 < prev_ema21 and ema9 > ema21 and macd_hist > 0 and vol_ratio >= 1.5`

*─ Momentum ─*
`close > vwap and ema9 > ema21 and macd_hist > prev_macd_hist and vol_ratio >= 1.5`

*─ Trend Following ─*
`adx > 25 and di_plus > di_minus and close > ema21 and macd_hist > 0`
`close > sma200 and ema9 > ema21 and vol_ratio >= 1.5`

*─ Bollinger ─*
`bb_bw < 0.03 and vol_ratio < 0.8` — Squeeze
`close > bb_top and vol_ratio >= 2` — Breakout atas
`close < bb_bottom and rsi < 30` — Oversold pantul

*─ Stoch Cross ─*
`prev_stoch_k < prev_stoch_d and stoch_k > stoch_d and stoch_k < 30`
_(golden cross stochastic di area oversold)_

*─ Cara Pakai ─*
`/scr (close > hhv(20) + vol_ratio >= 2 + rsi > 50) Breakout 20H`
`/algo (prev_close < prev_open + close > open + open <= prev_close + close >= prev_open + vol_ratio >= 1.5) Bullish Engulfing`
"""

TEKS_HELP = """
*🤖 BOT RADAR SAHAM IHSG*

*/scan* TICKER \\[TF\\] — Analisis \\+ chart
  TF: `1d` \\(default\\), `5m`, `15m`, `1h`

*/ihsg* — Status IHSG live
*/mf* KODE — Money flow hari ini

*/swing* — Screener swing daily \\(admin\\)
*/scalping* — Screener scalping 5m \\(admin, market buka\\)
*/hetrik* \\[bull\\|bear\\] \\[score\\=N\\] — 7\\-Candle Cycle scanner

*/scr* \\(f1 \\+ f2 \\+ \\.\\.\\.\\) Title — Custom screener
*/algo* \\(f1 \\+ f2\\) Title — Tambah algo otomatis
*/algo* list/stop/del ID — Kelola algo

*/daftar* — Daftar sebagai member
*/formula* — Daftar variabel lengkap
*/contoh* — Contoh formula siap pakai

Pisah formula dengan *\\+* bukan *and*
"""

# ============================================================
#  SEND HELPERS
# ============================================================
async def send_long(message, text: str, parse_mode='Markdown', edit=True):
    """Kirim teks panjang; split per baris, jaga integritas blok \\`\\`\\`."""
    lines = text.split('\n'); chunks = []
    buf_lines = []; buf_len = 0; in_code = False
    for line in lines:
        toggles = line.count('```'); will_toggle = (toggles % 2 == 1)
        line_len = len(line) + 1; overhead = 4 if in_code else 0
        if buf_len + line_len + overhead > TG_MAX and buf_lines:
            if in_code: buf_lines.append('```')
            chunks.append('\n'.join(buf_lines))
            buf_lines = ['```'] if in_code else []; buf_len = 4 if in_code else 0
        buf_lines.append(line); buf_len += line_len
        if will_toggle: in_code = not in_code
    if buf_lines:
        if in_code: buf_lines.append('```')
        chunks.append('\n'.join(buf_lines))
    for i, chunk in enumerate(chunks):
        try:
            if i == 0 and edit: await safe_edit(message, chunk, parse_mode=parse_mode)
            else:               await message.reply_text(chunk, parse_mode=parse_mode)
        except Exception:
            plain = chunk.replace('```', '').replace('*', '').replace('`', '')
            if i == 0 and edit: await safe_edit(message, plain)
            else:               await message.reply_text(plain)



# ============================================================
#  COMMAND HANDLERS
# ============================================================

# ── /start & /help ────────────────────────────────────────────
async def cmd_start(u: Update, _):
    await u.message.reply_text(TEKS_HELP, parse_mode='MarkdownV2')

async def cmd_help(u: Update, _):
    await u.message.reply_text(TEKS_HELP, parse_mode='MarkdownV2')

async def cmd_formula(u: Update, _):
    if not await admin_only(u): return
    await u.message.reply_text(TEKS_FORMULA, parse_mode='Markdown')

async def cmd_contoh(u: Update, _):
    if not await admin_only(u): return
    bagian = TEKS_CONTOH.split('\n\n*Trend Following:*')
    await u.message.reply_text(bagian[0], parse_mode='Markdown')
    if len(bagian) > 1:
        await u.message.reply_text('*Trend Following:*\n' + bagian[1], parse_mode='Markdown')

# ── /daftar ───────────────────────────────────────────────────
async def cmd_daftar(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = u.effective_user.id
    nama = u.effective_user.full_name or str(uid)

    if uid in ADMIN_IDS:
        return await u.message.reply_text("✅ Kamu adalah admin, tidak perlu daftar.")

    status = db_daftar(uid, nama)
    if status == 'banned':
        await u.message.reply_text(
            "⛔ Pendaftaran tidak dapat diproses.\n"
            "Hubungi admin untuk informasi lebih lanjut.")
    elif status == 'sudah':
        await u.message.reply_text(f"✅ *{nama}* sudah terdaftar sebagai member.",
                                   parse_mode='Markdown')
    elif status == 'pending':
        await u.message.reply_text(
            f"⏳ *{nama}*, pendaftaran kamu masih menunggu persetujuan admin.",
            parse_mode='Markdown')
    else:  # ok
        await u.message.reply_text(
            f"✅ *{nama}*, permintaan daftarmu sudah terkirim ke admin!\n"
            f"Tunggu konfirmasi, kamu akan bisa menggunakan bot setelah disetujui.\n\n"
            f"📋 User ID kamu: `{uid}`",
            parse_mode='Markdown')
        # Notif ke semua admin
        notif = (f"📨 *Pendaftaran Baru!*\n"
                 f"Nama : `{nama}`\n"
                 f"ID   : `{uid}`\n\n"
                 f"Ketik `/addmember {uid}` untuk approve.")
        for admin_id in ADMIN_IDS:
            try:
                await ctx.bot.send_message(chat_id=admin_id, text=notif, parse_mode='Markdown')
            except Exception:
                pass

# ── /addmember ────────────────────────────────────────────────
async def cmd_addmember(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(u): return

    if not ctx.args:
        # Tampilkan daftar pending
        pending = db_get_pending()
        if not pending:
            return await u.message.reply_text("📋 Tidak ada pendaftaran pending.")
        lines = ["*📋 Daftar Pending:*\n"]
        for uid2, nm, ts in pending:
            tgl = datetime.fromtimestamp(ts).strftime('%d/%m %H:%M')
            lines.append(f"• `{uid2}` — {nm} ({tgl})\n  `/addmember {uid2}`")
        await u.message.reply_text('\n'.join(lines), parse_mode='Markdown')
        return

    try:
        target_id = int(ctx.args[0])
    except ValueError:
        return await u.message.reply_text("⚠️ Format: `/addmember USER_ID [NAMA]`",
                                          parse_mode='Markdown')

    nama_override = ' '.join(ctx.args[1:]) if len(ctx.args) > 1 else None

    # Coba approve dari pending dulu
    ok, nama = db_approve(target_id)
    if not ok:
        # Tidak ada di pending → tambah langsung
        nama = nama_override or f"User_{target_id}"
        db_add_member_direct(target_id, nama)
        ok = True
    elif nama_override:
        nama = nama_override
        db_add_member_direct(target_id, nama)

    await u.message.reply_text(
        f"✅ *{nama}* (`{target_id}`) berhasil ditambahkan sebagai member.",
        parse_mode='Markdown')
    # Notif ke user
    try:
        await ctx.bot.send_message(
            chat_id=target_id,
            text=f"🎉 *Selamat!* Pendaftaran kamu telah disetujui.\n"
                 f"Ketik /help untuk melihat daftar perintah.",
            parse_mode='Markdown')
    except Exception:
        pass

# ── /listmember ──────────────────────────────────────────────
async def cmd_listmember(u: Update, _):
    if not await admin_only(u): return
    members = db_get_members()
    if not members:
        return await u.message.reply_text("📋 Belum ada member terdaftar.")
    lines = [f"*👥 Member Aktif ({len(members)}):*\n"]
    for uid2, nm in members:
        lines.append(f"• `{uid2}` — {nm}")
    await u.message.reply_text('\n'.join(lines), parse_mode='Markdown')

# ── /listpending ─────────────────────────────────────────────
async def cmd_listpending(u: Update, _):
    if not await admin_only(u): return
    pending = db_get_pending()
    if not pending:
        return await u.message.reply_text("📋 Tidak ada pendaftaran pending.")
    lines = [f"*⏳ Pending ({len(pending)}):*\n"]
    for uid2, nm, ts in pending:
        tgl = datetime.fromtimestamp(ts).strftime('%d/%m %H:%M')
        lines.append(f"• `{uid2}` — {nm} ({tgl})")
    await u.message.reply_text('\n'.join(lines), parse_mode='Markdown')

# ── /kick ────────────────────────────────────────────────────
async def cmd_kick(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(u): return
    if not ctx.args or not ctx.args[0].lstrip('-').isdigit():
        return await u.message.reply_text("Format: `/kick USER_ID`", parse_mode='Markdown')
    target_id = int(ctx.args[0])
    if target_id in ADMIN_IDS:
        return await u.message.reply_text("⛔ Tidak bisa kick admin.")
    ok = db_kick_member(target_id)
    if ok:
        await u.message.reply_text(f"🚫 User `{target_id}` di-kick dan di-blacklist.",
                                   parse_mode='Markdown')
    else:
        await u.message.reply_text(f"⚠️ User `{target_id}` tidak ditemukan.", parse_mode='Markdown')

# ── /notice ──────────────────────────────────────────────────
async def cmd_notice(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(u): return
    if not ctx.args:
        return await u.message.reply_text(
            "Format: `/notice PESAN`\nContoh: `/notice Maintenance besok pukul 12.00`",
            parse_mode='Markdown')

    teks   = ' '.join(ctx.args)
    members = db_get_members()
    if not members:
        return await u.message.reply_text("⚠️ Tidak ada member yang terdaftar.")

    p = await u.message.reply_text(
        f"📢 Mengirim ke {len(members)} member...", parse_mode='Markdown')

    pesan  = f"📢 *PENGUMUMAN*\n\n{teks}\n\n⏰ {datetime.now(WIB).strftime('%d %b %Y %H:%M WIB')}"
    berhasil = 0; gagal = 0
    for uid2, nm in members:
        try:
            await ctx.bot.send_message(chat_id=int(uid2), text=pesan, parse_mode='Markdown')
            berhasil += 1
            await asyncio.sleep(0.05)  # throttle agar tidak kena flood limit
        except Exception:
            gagal += 1

    await safe_edit(p, 
        f"✅ *Notice terkirim!*\n"
        f"Berhasil : {berhasil}\n"
        f"Gagal    : {gagal} _(user belum pernah DM bot)_",
        parse_mode='Markdown')

# ── /ihsg ────────────────────────────────────────────────────
async def cmd_ihsg(u: Update, _):
    if not await allowed_only(u): return
    p = await u.message.reply_text("📊 Mengambil data IHSG...")
    info, hist = await fetch_ihsg()
    if info is None:
        return await safe_edit(p, "❌ Gagal mengambil data IHSG.")

    price = float(info.get('regularMarketPrice') or info.get('currentPrice') or 0)
    prev  = float(info.get('regularMarketPreviousClose') or 0)
    chg   = price - prev
    chg_p = chg / prev * 100 if prev else 0
    high  = float(info.get('regularMarketDayHigh') or 0)
    low_  = float(info.get('regularMarketDayLow') or 0)
    vol   = float(info.get('regularMarketVolume') or 0)

    arrow = '🟢 ▲' if chg >= 0 else '🔴 ▼'
    sign  = '+' if chg >= 0 else ''

    # Trend 5 hari dari history
    trend_5d = ''
    if hist is not None and len(hist) >= 2:
        closes = hist['Close'].values
        d5 = float(closes[-1]) - float(closes[0])
        trend_5d = f"\nTrend 5D : {'+' if d5>=0 else ''}{d5:,.2f}  ({'🟢' if d5>=0 else '🔴'})"

    text = (
        f"📊 *IHSG — Indeks Harga Saham Gabungan*\n"
        f"```\n"
        f"Harga   : {price:>10,.2f}\n"
        f"Perubh  : {sign}{chg:>+.2f}  ({sign}{chg_p:.2f}%)\n"
        f"H / L   : {high:>10,.2f} / {low_:,.2f}\n"
        f"Volume  : {vol/1e9:>10.3f}B{trend_5d}\n"
        f"```\n"
        f"{arrow} {sign}{chg:.2f} ({sign}{chg_p:.2f}%)\n"
        f"⏰ {datetime.now(WIB).strftime('%H:%M WIB %d/%m/%Y')}"
    )
    await safe_edit(p, text, parse_mode='Markdown')

# ── /mf ──────────────────────────────────────────────────────
async def cmd_mf(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await allowed_only(u): return
    if not ctx.args:
        return await u.message.reply_text(
            "Format: `/mf KODE`\nContoh: `/mf BBCA`", parse_mode='Markdown')

    ticker = ctx.args[0].upper()
    p = await u.message.reply_text(
        f"💰 Menganalisis money flow *{mde(ticker)}*\\.\\.\\.", parse_mode='MarkdownV2')

    data = await fetch_mf(ticker)
    if data is None:
        return await safe_edit(p, 
            f"❌ Data tidak tersedia untuk *{ticker}*\n"
            f"_(Data 1m hanya tersedia saat/setelah market buka)_",
            parse_mode='Markdown')

    net_sign = '+' if data['net'] >= 0 else ''
    chg_sign = '+' if data['chg_pct'] >= 0 else ''

    # Bar ASCII yang kompatibel semua font Telegram
    def _bar(pct, width=15):
        filled = round(pct / 100 * width)
        return '[' + '=' * filled + '-' * (width - filled) + ']'

    text = (
        f"💰 *Money Flow — {data['ticker']}*\n"
        f"_Data hari ini ({data['candles']} candle 1m)_\n\n"
        f"```\n"
        f"Harga    : {data['last']:>10,.0f}  ({chg_sign}{data['chg_pct']:.2f}%)\n"
        f"\n"
        f"BUY  {data['buy_pct']:>5.1f}%  {_bar(data['buy_pct'])}\n"
        f"     {data['buy_vol']/1e6:>7.2f}M\n"
        f"SELL {data['sell_pct']:>5.1f}%  {_bar(data['sell_pct'])}\n"
        f"     {data['sell_vol']/1e6:>7.2f}M\n"
        f"\n"
        f"Net      : {net_sign}{data['net']/1e6:.2f}M\n"
        f"```\n"
        f"Verdict  : *{data['verdict']}*\n"
        f"⏰ {datetime.now(WIB).strftime('%H:%M WIB %d/%m/%Y')}"
    )
    await safe_edit(p, text, parse_mode='Markdown')

# ── /scan ─────────────────────────────────────────────────────
async def cmd_scan(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await allowed_only(u): return

    uid        = u.effective_user.id
    admin      = uid in ADMIN_IDS

    # Admin: tanpa cooldown. Member: cooldown 15 detik (anti double-tap saja).
    _SCAN_CD   = 0 if admin else 15
    sisa_scan  = max(0, int(_SCAN_CD - (time.time() - _SCAN_COOLDOWNS.get(uid, 0))))
    if sisa_scan > 0:
        return await u.message.reply_text(
            f"⏱ Tunggu {sisa_scan}s sebelum /scan berikutnya.")

    if not ctx.args:
        return await u.message.reply_text(
            "Gunakan: `/scan TICKER [TF]`\nContoh: `/scan BBRI 1d`",
            parse_mode='Markdown')

    ticker = ctx.args[0].upper()
    tf     = ctx.args[1].lower() if len(ctx.args) > 1 else '1d'

    _SCAN_COOLDOWNS[uid] = time.time()

    p = await u.message.reply_text(
        f"⏳ Menganalisis *{mde(ticker)}* \\({mde(tf.upper())}\\)\\.\\.\\.",
        parse_mode='MarkdownV2')

    loop  = asyncio.get_event_loop()
    hasil, err = await loop.run_in_executor(_IO_POOL, analisis_snr, ticker, tf)
    if err:
        return await safe_edit(p, f"❌ {err}")
    try:
        buf = await loop.run_in_executor(_IO_POOL, buat_chart, hasil)
        await p.delete()
        await u.message.reply_photo(
            photo=buf,
            caption=format_single(hasil),
            parse_mode='Markdown')
    except Exception as e:
        await safe_edit(p, f"❌ Error chart: {e}")

# ── /swing & /scalping ────────────────────────────────────────
async def cmd_dsi_scan(u: Update, ctx: ContextTypes.DEFAULT_TYPE, mode: str):
    """
    v7 (v3.0) — Tiered scanning:
      Tier 1: fast_gate (EMA+ST+vol) — filter ~80% ticker tanpa build_context penuh
      Tier 2: dsi_screener_df — full eval hanya ticker lolos tier 1
    Fix: gate c[-1]>=700 dihapus → sekarang proper liquidity gate.
    """
    if not await admin_only(u): return
    if mode == 'scalping' and not is_market_open():
        return await u.message.reply_text(
            "⚠️ Scalping screener hanya saat market buka \\(Sen\\-Jum 09:00\\-15:30 WIB\\)\\.",
            parse_mode='MarkdownV2')

    interval = '5m' if mode == 'scalping' else '1d'
    period   = '5d' if mode == 'scalping' else '6mo'

    loop = asyncio.get_event_loop()

    n_total = len(IDX_STOCKS)
    p = await u.message.reply_text(
        f"🔍 *{mode.upper()} Screener* \\| {n_total} saham\\.\\.\\.",
        parse_mode='MarkdownV2')

    # ── Download semua IDX_STOCKS via YF ─────────────────────────────────────
    try:
        await safe_edit(p, 
            f"📊 *{mode.upper()}*: Prefetch {n_total} saham \\(paralel\\)\\.\\.\\.",
            parse_mode='MarkdownV2')
    except Exception:
        pass

    batches = [IDX_STOCKS[i:i + SCAN_BATCH] for i in range(0, n_total, SCAN_BATCH)]
    all_dfs = await prefetch_parallel(batches, interval, period, n_parallel=int(os.getenv("N_PARALLEL", 6)))
    yf_hit  = len(all_dfs)
    logger.info("%s screener v3: YF %d/%d ticker OK", mode.upper(), yf_hit, n_total)

    # ── Tiered eval ───────────────────────────────────────────────────────────
    def _eval_all_dsi_tiered(items):
        out = []
        tier1_pass = 0
        for ticker, df in items:
            if len(df) < 40:
                continue
            # Tier 1: fast_gate (scalping tidak pakai fast_gate karena 5m beda dinamika)
            if mode == 'swing' and not fast_gate(df):
                continue
            tier1_pass += 1
            # Tier 2: full eval
            r, ok = dsi_screener_df(ticker, df, mode)
            if ok:
                out.append(r)
        logger.info("%s tiered: tier1=%d -> final=%d", mode.upper(), tier1_pass, len(out))
        return out

    items = [(t, df) for t, df in all_dfs.items() if len(df) >= 40]
    found = []
    if items:
        found = await loop.run_in_executor(_BATCH_EXEC, _eval_all_dsi_tiered, items)

    # ── Format hasil ─────────────────────────────────────────────────────────
    _ts       = datetime.now(WIB).strftime('%H:%M WIB %d/%m/%Y')
    _data_src = f"YF:{yf_hit}/{n_total}"

    if not found:
        try:
            await safe_edit(p, 
                f"*{mode.upper()} SCREENER*\n\n_Tidak ada saham memenuhi kriteria._\n\n"
                f"Data: {_data_src}\n⏰ {_ts}",
                parse_mode='Markdown')
        except Exception:
            await safe_edit(p, f"{mode.upper()} SCREENER\n\nTidak ada saham memenuhi kriteria.")
        return

    if mode == 'swing':
        kr   = "ST🟢|EMA9>21|MACD+|RSI40-75|ADX>18|Vol≥1.3x|MFI>55|Score≥6 (v3.0)"
        hdr  = f"{'Ticker':<6}  {'Harga':>8}  {'Scr':>4}  {'RSI':>5}  {'VR':>4}  {'CMF':>6}  {'Sig'}"
        sort_key = lambda x: x.get('score', 0)
        def fmt_row(r):
            mc  = "M" if r.get('macd_cross') else " "
            sc  = "S" if r.get('stoch_cross') else " "
            sq  = "Q" if r.get('sqz_off')    else " "
            dv  = "D" if r.get('div_bull')    else " "
            ha  = "H" if r.get('ha_streak', 0) >= 3 else " "
            cmf_v = r.get('cmf', 0.0)
            return (f"{r['ticker']:<6}  {r['close']:>8,.0f}  "
                    f"{r.get('score',0):>3.1f}  {r['rsi']:>5.1f}  "
                    f"{r['vol_ratio']:>3.1f}x  {cmf_v:>+5.2f}  {mc}{sc}{sq}{dv}{ha}")
    else:
        kr   = "Close>VWAP | EMA9>21 | MACD>0 | Vol>=1.5x | RSI 50-75 | ST🟢 (5m)"
        hdr  = f"{'Ticker':<6}  {'Harga':>8}  {'RSI':>5}  {'VR':>4}  {'ADX':>5}  {'CMF':>6}"
        sort_key = lambda x: x['vol_ratio']
        def fmt_row(r):
            cmf_v = r.get('cmf', 0.0)
            return (f"{r['ticker']:<6}  {r['close']:>8,.0f}  "
                    f"{r['rsi']:>5.1f}  {r['vol_ratio']:>3.1f}x  {r['adx']:>5.1f}  {cmf_v:>+5.2f}")

    lines_out = [
        f"*HASIL {mode.upper()} SCREENER* — Found: *{len(found)}*",
        f"_{kr}_",
        f"Data: {_data_src}",
    ]
    if mode == 'swing':
        lines_out.append("_Sig: M=MACD Cross S=Stoch Q=SqzBreak D=Divergence H=HA Streak_")
    lines_out += ["```", hdr]
    for r in sorted(found, key=sort_key, reverse=True)[:30]:
        lines_out.append(fmt_row(r))
    lines_out.append("```")
    lines_out.append(f"⏰ {_ts}")
    text = '\n'.join(lines_out)

    try:
        await safe_edit(p, text, parse_mode='Markdown')
    except Exception as _e1:
        logger.warning("dsi_scan edit_text gagal: %s", _e1)
        try:
            await p.reply_text(text, parse_mode='Markdown')
        except Exception as _e2:
            logger.error("dsi_scan reply_text juga gagal: %s", _e2)
            await p.reply_text(text.replace('*','').replace('_','').replace('`',''))


async def cmd_swing(u, c):
    if not await admin_only(u): return
    await cmd_dsi_scan(u, c, 'swing')

async def cmd_scalping(u, c):
    if not await admin_only(u): return
    await cmd_dsi_scan(u, c, 'scalping')
# ── /scr ─────────────────────────────────────────────────────
async def cmd_scr(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(u): return
    uid = u.effective_user.id
    sisa = check_cooldown(uid)
    if sisa > 0:
        return await u.message.reply_text(f"⏱ Tunggu {sisa} detik lagi sebelum /scr berikutnya.")
    if not ctx.args:
        return await u.message.reply_text(
            "Format: `/scr (f1 + f2 + ...) Judul`\n\n"
            "Contoh:\n`/scr (close > hhv(20) + vol_ratio >= 2 + rsi > 50) Breakout 20H`\n\n"
            "Lihat /contoh untuk ide formula.", parse_mode='Markdown')

    formula, title = parse_scr_input(ctx.args)
    if formula is None:
        return await u.message.reply_text(
            "❌ Format salah\\. Gunakan: `/scr \\(f1 \\+ f2\\) Judul`",
            parse_mode='MarkdownV2')

    ok, err = validate_formula(formula)
    if not ok:
        return await u.message.reply_text(f"❌ Formula tidak valid:\n`{err}`\n\nCek /formula",
                                          parse_mode='Markdown')

    set_cooldown(uid)
    t0 = time.time()
    p  = await u.message.reply_text(
        f"🔍 *{mde(title)}*\nScanning {len(IDX_STOCKS)} saham\\.\\.\\.",
        parse_mode='MarkdownV2')

    found = []
    batches_list = [IDX_STOCKS[i:i + SCAN_BATCH] for i in range(0, len(IDX_STOCKS), SCAN_BATCH)]

    # Progress awal
    try:
        _prog = f"🔍 *{mde(title)}*\nPrefetch {len(IDX_STOCKS)} saham \\(paralel\\)\\.\\.\\."
        await safe_edit(p, _prog, parse_mode='MarkdownV2')
    except Exception:
        pass

    # Download semua ticker paralel (3 batch × 50 = 150 ticker simultan)
    all_dfs = await prefetch_parallel(batches_list, '1d', '6mo', n_parallel=int(os.getenv("N_PARALLEL", 6)))

    # Lazy indicator: hitung hanya grup yang dibutuhkan formula ini
    _lazy_groups = extract_needed_groups(formula)

    def _eval_all_scr(items):
        out = []
        for ticker, df in items:
            if len(df) < 40: continue
            ctx2 = build_context(df, ticker=ticker, needed_groups=_lazy_groups)
            if ctx2 and eval_formula(formula, ctx2):
                out.append({
                    'ticker': ticker, 'close': ctx2['close'],
                    'vol_ratio': ctx2['vol_ratio'], 'rsi': ctx2['rsi'],
                    'macd_hist': ctx2.get('macd_hist', 0.0), 'adx': ctx2.get('adx', 0.0),
                    'cmf': ctx2.get('cmf', 0.0),
                })
        return out

    loop2 = asyncio.get_event_loop()
    found = await loop2.run_in_executor(_CPU_POOL, _eval_all_scr, list(all_dfs.items()))
    elapsed = time.time() - t0
    fl = formula if len(formula) <= 60 else formula[:57] + '...'

    if not found:
        return await safe_edit(p, 
            f"*{title}*\n`{fl}`\n\n_Tidak ada saham memenuhi kriteria_\n"
            f"⏱ Selesai dalam {elapsed:.0f}s", parse_mode='Markdown')

    lines = [
        f"🔍 *{title}*", f"`{fl}`",
        f"Scan: {len(IDX_STOCKS)} saham | Ditemukan: *{len(found)}* | ⏱ {elapsed:.0f}s", "```",
        f"{'Ticker':<6}  {'Harga':>8}  {'VR':>5}  {'RSI':>5}  {'ADX':>5}  {'MACD H':>7}"
    ]
    for r in sorted(found, key=lambda x: x['vol_ratio'], reverse=True):
        mh = f"+{r['macd_hist']:.1f}" if r['macd_hist'] >= 0 else f"{r['macd_hist']:.1f}"
        lines.append(
            f"{r['ticker']:<6}  {r['close']:>8,.0f}  "
            f"{r['vol_ratio']:>4.1f}×  {r['rsi']:>5.1f}  {r['adx']:>5.1f}  {mh:>7}")
    lines.append("```")
    lines.append(f"⏰ {datetime.now(WIB).strftime('%d %b %Y %H:%M WIB')}")
    await send_long(p, '\n'.join(lines), parse_mode='Markdown')

# ── /algo ─────────────────────────────────────────────────────
async def cmd_algo(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not await admin_only(u): return
    uid = u.effective_user.id
    cid = u.effective_user.id  # FIX: selalu pakai user_id agar sinyal bisa DM ke owner
    if not ctx.args:
        return await u.message.reply_text(
            "*Penggunaan /algo:*\n\n"
            "`/algo (f1 + f2) Judul` — Tambah algo\n"
            "`/algo list` — Lihat daftar\n"
            "`/algo stop ID` — Nonaktifkan\n"
            "`/algo del ID` — Hapus\n\n"
            "Lihat /contoh untuk ide formula.", parse_mode='Markdown')

    sub = ctx.args[0].lower()

    if sub == 'list':
        conn = db_conn()
        rows = conn.execute(
            "SELECT id,title,formula,active FROM algos WHERE user_id=? ORDER BY id DESC LIMIT 10",
            (uid,)).fetchall()
        conn.close()
        if not rows:
            return await u.message.reply_text(
                "Belum ada algo. Tambah dengan `/algo (formula) Judul`",
                parse_mode='Markdown')
        lines = ["*📋 Algo kamu:*\n"]
        for rid, title, formula, active in rows:
            st = "✅ aktif" if active else "⏸ nonaktif"
            fl = formula if len(formula) <= 55 else formula[:52] + '...'
            lines.append(f"`[{rid}]` *{title}* ({st})\n`{fl}`\n")
        return await u.message.reply_text('\n'.join(lines), parse_mode='Markdown')

    if sub == 'stop':
        if len(ctx.args) < 2 or not ctx.args[1].isdigit():
            return await u.message.reply_text("Gunakan: `/algo stop ID`", parse_mode='Markdown')
        aid = int(ctx.args[1])
        conn = db_conn()
        row = conn.execute("SELECT id FROM algos WHERE id=? AND user_id=?", (aid, uid)).fetchone()
        if not row: conn.close(); return await u.message.reply_text("❌ Algo tidak ditemukan.")
        conn.execute("UPDATE algos SET active=0 WHERE id=?", (aid,))
        conn.commit(); conn.close()
        return await u.message.reply_text(f"⏸ Algo #{aid} dinonaktifkan.")

    if sub == 'del':
        if len(ctx.args) < 2 or not ctx.args[1].isdigit():
            return await u.message.reply_text("Gunakan: `/algo del ID`", parse_mode='Markdown')
        aid = int(ctx.args[1])
        conn = db_conn()
        row = conn.execute("SELECT id FROM algos WHERE id=? AND user_id=?", (aid, uid)).fetchone()
        if not row: conn.close(); return await u.message.reply_text("❌ Algo tidak ditemukan.")
        conn.execute("DELETE FROM algos WHERE id=?", (aid,))
        conn.execute("DELETE FROM algo_fired WHERE algo_id=?", (aid,))
        conn.commit(); conn.close()
        return await u.message.reply_text(f"🗑️ Algo #{aid} dihapus.")

    # Tambah algo baru
    formula, title = parse_scr_input(ctx.args)
    if formula is None:
        return await u.message.reply_text(
            "❌ Format salah. Gunakan:\n`/algo (f1 + f2) Judul`", parse_mode='Markdown')

    ok, err = validate_formula(formula)
    if not ok:
        return await u.message.reply_text(
            f"❌ Formula tidak valid:\n`{err}`\n\nCek /formula", parse_mode='Markdown')

    conn = db_conn()
    jumlah = conn.execute(
        "SELECT COUNT(*) FROM algos WHERE user_id=? AND active=1", (uid,)).fetchone()[0]
    if jumlah >= MAX_ALGOS:
        conn.close()
        return await u.message.reply_text(
            f"⚠️ Maksimal {MAX_ALGOS} algo aktif. Hapus dulu dengan `/algo del ID`.",
            parse_mode='Markdown')
    conn.execute(
        "INSERT INTO algos(user_id,chat_id,formula,title) VALUES(?,?,?,?)",
        (uid, cid, formula, title))
    conn.commit()
    aid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    await u.message.reply_text(
        f"✅ *Algo \\#{aid} disimpan\\!*\n\n*{mde(title)}*\n`{formula}`\n\n"
        f"Scan otomatis tiap {ALGO_INTERVAL} menit saat market buka\\.",
        parse_mode='MarkdownV2')


# ── /hetrik ───────────────────────────────────────────────────
async def cmd_hetrik(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /hetrik [bull|bear] [score=N]
    Scan IDX untuk 7-Candle Cycle (Hetrik) + GRG pattern.
    Gunakan IDX_LIQUID (~230 saham aktif) agar cepat.

    Contoh:
      /hetrik          → semua sinyal score >= 3
      /hetrik bull     → hanya bullish streak
      /hetrik bear     → hanya bearish streak
      /hetrik score=4  → filter score minimum 4
    """
    if not await allowed_only(u): return

    # Parse argumen
    args       = [a.lower() for a in (ctx.args or [])]
    filter_dir = None   # None = semua, 'green' = bull, 'red' = bear
    min_score  = 3

    for a in args:
        if a in ('bull', 'bullish', 'green', 'hijau'):
            filter_dir = 'green'
        elif a in ('bear', 'bearish', 'red', 'merah'):
            filter_dir = 'red'
        elif a.startswith('score='):
            try:
                min_score = int(a.split('=', 1)[1])
            except ValueError:
                pass

    p = await u.message.reply_text(
        f"🔍 Scanning hetrik pattern IDX Liquid (~{len(IDX_LIQUID)} ticker)...")

    tickers = IDX_LIQUID.copy()
    results = []
    loop    = asyncio.get_event_loop()
    sem     = asyncio.Semaphore(int(os.getenv('N_PARALLEL', 6)))

    async def _scan_one(ticker: str):
        async with sem:
            try:
                df = await loop.run_in_executor(
                    _IO_POOL, safe_download, ticker, '1d', '3mo')
                if df is None or len(df) < 20:
                    return
                sig = detect_hetrik_pattern(df)
                if not sig['hetrik']:
                    return
                if sig['hetrik_score'] < min_score:
                    return
                if filter_dir and sig['hetrik_bull'] != (filter_dir == 'green'):
                    return

                # Extra context untuk output
                c  = df['Close'].values
                v  = df['Volume'].values
                va = float(pd.Series(v).rolling(20).mean().values[-1]) or 1.0

                results.append({
                    **sig,
                    'ticker':    ticker,
                    'close':     float(c[-1]),
                    'vol_ratio': float(v[-1]) / va,
                })
            except Exception as _e:
                logger.debug("hetrik scan %s: %s", ticker, _e)

    await asyncio.gather(*[_scan_one(t) for t in tickers], return_exceptions=True)

    if not results:
        return await safe_edit(p, "❌ Tidak ada sinyal hetrik hari ini.")

    # Sort: score desc, lalu vol_ratio desc
    results.sort(key=lambda x: (x['hetrik_score'], x['vol_ratio']), reverse=True)

    now_str  = datetime.now(WIB).strftime('%d %b %Y %H:%M WIB')
    dir_str  = {'green': ' — BULL 🟢', 'red': ' — BEAR 🔴'}.get(filter_dir, '')
    lines    = [
        f"📊 *HETRIK SCANNER{dir_str}*",
        f"Score ≥ {min_score} | {len(results)} sinyal | {now_str}\n",
    ]

    _PHASE_LABEL = {
        3: 'C3-Entry', 4: 'C4-Exh', 5: 'C5-Retrace',
        6: 'C6-Push',  7: 'C7-Close',
    }

    for r in results[:15]:
        sym   = '🟢' if r['hetrik_bull'] else '🔴'
        phase = _PHASE_LABEL.get(r['hetrik_phase'], f"C{r['hetrik_phase']}")

        tags = []
        if r['hetrik_grg']:      tags.append('GRG')
        if r['hetrik_c4_exh']:   tags.append('Exh')
        if r['hetrik_c5_retrace']: tags.append('Ret')
        tag_str = ' '.join(tags)

        lines.append(
            f"{sym} *{r['ticker']}*  {phase}  Streak:{r['hetrik_streak']}x"
            f"  Skor:{r['hetrik_score']}  {tag_str}\n"
            f"  Harga:{r['close']:,.0f}  VolR:{r['vol_ratio']:.1f}x"
            f"  EntryRef:{r['hetrik_entry_ref']:,.0f}"
        )

    if len(results) > 15:
        lines.append(f"\n_...+{len(results)-15} sinyal lainnya (filter score={min_score})_")

    lines.append(
        "\n_Pakai `/algo (hetrik_bull + hetrik_phase==5 + htf_bull) Hetrik Retrace`"
        " untuk alert otomatis_"
    )

    await send_long(p, '\n'.join(lines), parse_mode='Markdown')


# ── /status ───────────────────────────────────────────────────
async def cmd_topcan(u: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /topcan          — tampilkan top gainer candidates dari EOD screen terakhir
    /topcan now      — admin: trigger manual EOD screen (30–60 detik)
    /topcan morning  — admin: trigger manual morning check sekarang
    """
    if not await allowed_only(u): return

    sub      = ctx.args[0].lower() if ctx.args else ''
    is_admin = u.effective_user.id in ADMIN_IDS

    if sub == 'now':
        if not is_admin:
            return await u.message.reply_text("⛔ Hanya admin.")
        p = await u.message.reply_text("⏳ EOD Screen berjalan… (~30–60 detik)")
        try:
            await jalankan_eod_screen(ctx.application)
            await p.delete()
        except Exception as e:
            await safe_edit(p, f"❌ Error: {e}")
        return

    if sub == 'morning':
        if not is_admin:
            return await u.message.reply_text("⛔ Hanya admin.")
        p = await u.message.reply_text("⏳ Morning trigger check…")
        try:
            await jalankan_morning_trigger(ctx.application)
            await p.delete()
        except Exception as e:
            await safe_edit(p, f"❌ Error: {e}")
        return

    # Default: tampilkan candidates
    conn = db_conn()
    rows = conn.execute(
        "SELECT ticker,score,close,vol_ratio,rsi,cmf,scl,swl,sektor,scan_date,am_fired "
        "FROM gainer_candidates ORDER BY score DESC LIMIT 25"
    ).fetchall()
    conn.close()

    if not rows:
        return await u.message.reply_text(
            "📭 Belum ada data.\n"
            "EOD screen otomatis jam *15:45 WIB* setiap hari kerja.\n"
            "Admin: `/topcan now` untuk trigger manual.",
            parse_mode='Markdown')

    scan_date = rows[0][9]
    fired_n   = sum(1 for r in rows if r[10])
    lines = [
        f"🎯 *Top Gainer Candidates* — {scan_date}",
        f"_{len(rows)} kandidat | Morning fired: {fired_n}_\n",
        f"`{'#':<3} {'Tick':<6} {'Scr':>4} {'Harga':>8} {'VR':>5} {'RSI':>5} {'CMF(T-1)':>8}`",
    ]
    for i, r in enumerate(rows, 1):
        ticker, sc, close, vr, rsi, cmf, scl, swl, sk, _, am = r
        icon  = "⭐" if sc >= 13 else ("🔸" if sc >= 11 else ("·" if sc >= 9 else " "))
        sk_t  = f" [{sk.upper()}]" if sk else ""
        am_t  = " ✅" if am else ""
        lines.append(
            f"`{i:<3}` {icon}`{ticker:<6}` {sc:>2}/17 "
            f"{close:>8,.0f} {vr:>4.1f}x {rsi:>5.1f} {cmf:>+5.2f}"
            f"{sk_t}{am_t}"
        )

    lines.append(
        f"\n_CMF/RSI/VR = data penutupan {scan_date} (T-1), bukan real-time._\n"
        "_Jadwal: EOD 15:45 | Pre-warm 08:55 | Morning 09:05_\n"
        "_Admin: `/topcan now` | `/topcan morning`_"
    )
    await u.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_status(u: Update, _):
    if not await admin_only(u): return
    conn = db_conn()
    n_member = conn.execute("SELECT COUNT(*) FROM members WHERE status='active'").fetchone()[0]
    n_pending = conn.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
    n_algo   = conn.execute("SELECT COUNT(*) FROM algos WHERE active=1").fetchone()[0]
    n_fired  = conn.execute("SELECT COUNT(*) FROM algo_fired WHERE fired_date=?",
                            (datetime.now(WIB).strftime('%Y-%m-%d'),)).fetchone()[0]
    conn.close()

    import platform, sys
    uptime_info = datetime.now(WIB).strftime('%d %b %Y %H:%M WIB')
    lines = [
        "*📡 BOT STATUS*",
        "```",
        f"DB Path  : {DB_PATH}",
        f"Data Dir : {DATA_DIR}",
        f"Python   : {sys.version.split()[0]}",
        f"Platform : {platform.system()} {platform.machine()}",
        "```",
        "*Database:*",
        f"Member aktif  : {n_member}",
        f"Pending       : {n_pending}",
        f"Algo aktif    : {n_algo}",
        f"Fired hari ini: {n_fired}",
        f"⏰ {uptime_info}",
    ]
    text = "\n".join(lines)
    await u.message.reply_text(text, parse_mode='Markdown')

# ============================================================
#  MAIN
# ============================================================
def main():
    if not BOT_TOKEN:
        raise ValueError("BOT_TOKEN kosong! Isi di file .env")

    db_init()
    app = Application.builder().token(BOT_TOKEN).build()

    # Daftarkan semua handler
    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("help",        cmd_help))
    app.add_handler(CommandHandler("formula",     cmd_formula))
    app.add_handler(CommandHandler("contoh",      cmd_contoh))
    app.add_handler(CommandHandler("scan",        cmd_scan))
    app.add_handler(CommandHandler("swing",       cmd_swing))
    app.add_handler(CommandHandler("scalping",    cmd_scalping))
    app.add_handler(CommandHandler("scr",         cmd_scr))
    app.add_handler(CommandHandler("algo",        cmd_algo))
    app.add_handler(CommandHandler("ihsg",        cmd_ihsg))
    app.add_handler(CommandHandler("mf",          cmd_mf))
    app.add_handler(CommandHandler("hetrik",      cmd_hetrik))
    app.add_handler(CommandHandler("daftar",      cmd_daftar))
    app.add_handler(CommandHandler("addmember",   cmd_addmember))
    app.add_handler(CommandHandler("listmember",  cmd_listmember))
    app.add_handler(CommandHandler("listpending", cmd_listpending))
    app.add_handler(CommandHandler("kick",        cmd_kick))
    app.add_handler(CommandHandler("notice",      cmd_notice))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("topcan",      cmd_topcan))

    async def on_startup(application):
        sched = AsyncIOScheduler(timezone="Asia/Jakarta")
        # Algo scan tiap ALGO_INTERVAL menit saat market buka
        sched.add_job(jalankan_algo_scan, trigger='cron',
                      minute=f'*/{ALGO_INTERVAL}', args=[application])
        # Cache evict tiap 10 menit
        sched.add_job(_cache_evict_job, trigger='interval', minutes=10)
        # ── Pre-warm: download semua IDX_STOCKS setiap hari 08:55 WIB ─────────
        # Setelah pre-warm selesai, /swing dan /scr tidak perlu download lagi —
        # langsung serve dari cache selama 30 menit (TTL daily = 1800s).
        sched.add_job(prewarm_all_tickers, trigger='cron',
                      hour=8, minute=55, day_of_week='mon-fri',
                      args=[application])
        # ── EOD Screen: score 957 saham → simpan top 75 kandidat (15:45) ─────
        sched.add_job(jalankan_eod_screen, trigger='cron',
                      hour=15, minute=45, day_of_week='mon-fri',
                      args=[application])
        # ── Morning Trigger: cek gap+vol live → alert (09:05 & 09:30) ────────
        sched.add_job(jalankan_morning_trigger, trigger='cron',
                      hour=9, minute=5, day_of_week='mon-fri',
                      args=[application])
        sched.add_job(jalankan_morning_trigger, trigger='cron',
                      hour=9, minute=30, day_of_week='mon-fri',
                      args=[application])
        sched.start()
        application.bot_data['scheduler'] = sched  # Prevent GC + survive Android Doze

        _np   = int(os.getenv("N_PARALLEL", 6))
        _iow  = int(os.getenv("IO_WORKERS", 30))
        _sd   = float(os.getenv("SCAN_DELAY", 0.05))
        print("=" * 55)
        print("  Bot Radar Saham IHSG — Termux Edition v3.1")
        print(f"  Data dir : {DATA_DIR}")
        print(f"  DB       : {DB_PATH}")
        print(f"  Admin    : {ADMIN_IDS}")
        print(f"  Allowed  : {ALLOWED_CHAT_IDS or '(member DB)'}")
        print(f"  Algo     : tiap {ALGO_INTERVAL} menit (market hours)")
        print(f"  Topic ID : {ALGO_TOPIC_ID or '(tidak diset, kirim ke chat)'}")
        print(f"  Chart BG : {CHART_BG_PATH or '(default dark theme)'}")
        print(f"  Scan     : {SCAN_BATCH} tk/batch | delay {_sd}s | {_np}x parallel")
        print(f"  Threads  : IO_WORKERS={_iow} | N_PARALLEL={_np}")
        print(f"  Pre-warm : 08:55 | EOD Screen: 15:45 | Morning: 09:05+09:30")
        print("=" * 55)
        print(f"  TwelData : {'OK (' + TWELVEDATA_API_KEY[:8] + '...)' if TWELVEDATA_API_KEY else 'tidak diset'}")
        print("=" * 55)
        # Verifikasi isi DB saat startup
        try:
            _vc = db_conn()
            _nm = _vc.execute("SELECT COUNT(*) FROM members WHERE status='active'").fetchone()[0]
            _na = _vc.execute("SELECT COUNT(*) FROM algos WHERE active=1").fetchone()[0]
            _vc.close()
            print(f"  DB loaded : {_nm} member, {_na} algo aktif")
            logger.info("Bot started. DATA_DIR=%s DB=%s members=%d algos=%d",
                        DATA_DIR, DB_PATH, _nm, _na)
        except Exception as _e:
            logger.warning("DB read error on startup: %s", _e)

    app.post_init = on_startup
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()

