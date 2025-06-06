#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YouTube-парсер: Shorts + обычные ролики, без live/upcoming.
Считывает ключевые слова (col A) и исходные URL (col J) из Google Sheets и сохраняет результаты обратно в лист "Results".
Продолжает с места остановки через shelve.
При исчерпании квоты всех ключей останавливается и записывает уже собранные данные.
Сохраняет прогресс каждые BATCH_SIZE ключей.
"""
import os
import sys
import shelve
import time
import logging
import socket
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import isodate
from langdetect import detect, LangDetectException
from dotenv import load_dotenv, dotenv_values
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account

# ─── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ─── Конфигурация (из .env) ─────────────────────────────────────
# Явно указываем путь к .env рядом со скриптом и перезаписываем системные переменные
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    logger.error(f"❌ .env файл не найден по пути {env_path}")
    sys.exit(1)

load_dotenv(dotenv_path=env_path, override=True)
SHEET_ID = os.getenv("SHEET_ID")
logger.info(f"🚩 Loaded SHEET_ID from {env_path}: {SHEET_ID}")

KEYWORDS_SHEET       = os.getenv("KEYWORDS_SHEET", "Keywords")
RESULTS_SHEET        = os.getenv("RESULTS_SHEET", "Results")
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")
NUM_RESULTS          = int(os.getenv("NUM_RESULTS", 10))
REGION               = os.getenv("REGION", "US")
MAX_PAGES            = int(os.getenv("MAX_PAGES", 1))
OUTPUT_CSV           = os.getenv("OUTPUT_CSV", "yt_results.csv")
BATCH_SIZE           = int(os.getenv("BATCH_SIZE", 50))

if MAX_PAGES < 1:
    sys.exit("❌ MAX_PAGES должен быть ≥ 1")

# ─── Google Sheets API ─────────────────────────────────────────
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
creds_sheets = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_JSON, scopes=SCOPES
)
sheets_service = build(
    'sheets', 'v4',
    credentials=creds_sheets,
    cache_discovery=False
)

# ─── Хранилище кеша ─────────────────────────────────────────────
CACHE = shelve.open("yt_cache.db")

# ─── Ошибка исчерпания квоты всех ключей ────────────────────────
class QuotaExceededAllKeys(Exception):
    """Все ключи YouTube API исчерпали квоту"""
    pass

# ─── Классы для управления API-ключами ────────────────────────────
class APIKey:
    def __init__(self, key: str):
        self.key = key
        self.service = build(
            "youtube", "v3",
            developerKey=key,
            cache_discovery=False
        )
        self.used_units = 0
        self.active = True

class KeyManager:
    def __init__(self, keys: list[str]):
        self.keys = [APIKey(k) for k in keys]
        self.index = 0

    def get_key(self) -> APIKey:
        n = len(self.keys)
        for _ in range(n):
            api = self.keys[self.index]
            self.index = (self.index + 1) % n
            if api.active:
                return api
        raise QuotaExceededAllKeys()

    def deactivate(self, api: APIKey):
        api.active = False
        logger.warning(f"Деактивирован ключ: {api.key}")

    def record(self, api: APIKey, units: int):
        api.used_units += units
        logger.info(f"Ключ {api.key}: расход {units} units (итого {api.used_units})")

    def execute(self, fn, units=0, backoff_max=3):
        attempt = 0
        while True:
            api = self.get_key()
            try:
                resp = fn(api.service).execute()
                used = units(resp) if callable(units) else units
                self.record(api, used)
                return resp
            except HttpError as e:
                err_str = str(e)
                if 'quotaExceeded' in err_str or 'dailyLimitExceeded' in err_str:
                    self.deactivate(api)
                    continue
                if 'rateLimitExceeded' in err_str:
                    if attempt < backoff_max:
                        delay = 2 ** attempt
                        logger.warning(
                            f"Rate limitExceeded на ключе {api.key}, жду {delay}s (попытка {attempt+1})"
                        )
                        time.sleep(delay)
                        attempt += 1
                        continue
                    else:
                        self.deactivate(api)
                        continue
                logger.error(f"Unexpected HttpError на ключе {api.key}: {e}")
                raise
            except (ConnectionResetError, socket.error) as e:
                if attempt < backoff_max:
                    delay = 2 ** attempt
                    logger.warning(
                        f"Connection error на ключе {api.key}: {e}, жду {delay}s (попытка {attempt+1})"
                    )
                    time.sleep(delay)
                    attempt += 1
                    continue
                else:
                    self.deactivate(api)
                    continue

# ─── Настройка ключей YouTube API ────────────────────────────────
raw_keys = os.getenv("YT_API_KEYS") or dotenv_values(env_path).get("YT_API_KEYS", "")
KEYS = [k.strip() for k in raw_keys.split(",") if k.strip()]
if not KEYS:
    sys.exit("❌ Нет API-ключей (YT_API_KEYS).")
logger.info(f"🔑 Найдено {len(KEYS)} ключей")
key_manager = KeyManager(KEYS)

VIDEO_PARTS = "snippet,contentDetails,status,player,statistics"

# ─── Утилиты ─────────────────────────────────────────────────────
def iso2hms(iso: str):
    if not iso:
        return "", 0
    td = isodate.parse_duration(iso)
    s = int(td.total_seconds())
    h, r = divmod(s, 3600)
    m, sec = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}", s

def fmt_age(pub: str):
    if not pub:
        return ""
    dt = datetime.fromisoformat(pub.replace('Z', '+00:00'))
    d = (datetime.now(timezone.utc) - dt).days
    y, rem = divmod(d, 365)
    mo = rem // 30
    return f"{y} years {mo} months" if y else f"{mo} months"

def safe_detect(txt: str):
    try:
        return detect(txt) if txt.strip() else ""
    except LangDetectException:
        return ""

# ─── Функции для записи в Google Sheets и CSV ───────────────────
def append_to_sheets_with_retry(service, spreadsheet_id, sheet_range, values,
                                max_retries=5, base_delay=1):
    for attempt in range(1, max_retries+1):
        try:
            service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                valueInputOption='RAW',
                body={'values': values}
            ).execute()
            logger.info("✅ Успешно записали результаты в Google Sheets")
            return True
        except (HttpError, ConnectionResetError, socket.error) as e:
            delay = base_delay * 2**(attempt-1)
            logger.warning(
                f"⚠️ Попытка {attempt}/{max_retries} записи в Sheets упала: {e}. Ждём {delay}s."
            )
            time.sleep(delay)
    logger.error("❌ Не удалось записать в Google Sheets после всех попыток")
    return False

def save_csv_with_retry(df: pd.DataFrame, path: str, max_retries=3, base_delay=1):
    for attempt in range(1, max_retries+1):
        try:
            df.to_csv(path, index=False)
            logger.info(f"✅ CSV сохранён: {path}")
            return True
        except Exception as e:
            delay = base_delay * 2**(attempt-1)
            logger.warning(
                f"⚠️ Попытка {attempt}/{max_retries} сохранения CSV упала: {e}. Ждём {delay}s."
            )
            time.sleep(delay)
    logger.error("❌ Не удалось сохранить CSV после всех попыток")
    return False

def append_csv_batch(df: pd.DataFrame, path: str, max_retries=3, base_delay=1) -> bool:
    """Дозаписывает DataFrame в CSV, создавая файл с заголовком, если не существует."""
    p = Path(path)
    header = not p.exists()
    for attempt in range(1, max_retries+1):
        try:
            df.to_csv(path, index=False, header=header, mode='a')
            logger.info(f"✅ CSV батч сохранён ({len(df)} строк): {path}")
            return True
        except Exception as e:
            delay = base_delay * 2**(attempt-1)
            logger.warning(f"⚠️ Попытка {attempt}/{max_retries} дозаписи CSV упала: {e}. Ждём {delay}s.")
            time.sleep(delay)
    logger.error("❌ Не удалось дозаписать CSV после всех попыток")
    return False

# ─── Поиск YouTube видео по ключу ────────────────────────────────
def search_once(keyword: str, input_url: str) -> list:
    rows, token, page = [], None, 0
    while len(rows) < NUM_RESULTS and page < MAX_PAGES:
        page += 1
        sk = f"S:{keyword}:{REGION}:{token or ''}"
        if sk in CACHE:
            sr = CACHE[sk]
        else:
            sr = key_manager.execute(
                lambda svc: svc.search().list(
                    q=keyword,
                    part="id",
                    type="video",
                    order="relevance",
                    regionCode=REGION,
                    maxResults=min(50, NUM_RESULTS - len(rows)),
                    pageToken=token or "",
                    safeSearch="none"
                ),
                units=100
            )
            CACHE[sk] = sr
        vids = [it.get('id', {}).get('videoId') for it in sr.get('items', []) if it.get('id', {}).get('videoId')]
        if not vids:
            break
        vk = f"V:{','.join(vids)}"
        if vk in CACHE:
            vr = CACHE[vk]
        else:
            vr = key_manager.execute(
                lambda svc: svc.videos().list(
                    id=",".join(vids),
                    part=VIDEO_PARTS
                ),
                units=lambda resp: len(resp.get('items', []))
            )
            CACHE[vk] = vr
        cids = list({it['snippet']['channelId'] for it in vr.get('items', [])})
        amap = {}
        if cids:
            ck = f"C:{','.join(cids)}"
            if ck in CACHE:
                ch = CACHE[ck]
            else:
                ch = key_manager.execute(
                    lambda svc: svc.channels().list(
                        part="snippet",
                        id=",".join(cids)
                    ),
                    units=1
                )
                CACHE[ck] = ch
            amap = {c['id']: c['snippet']['thumbnails']['default']['url'] for c in ch.get('items', [])}
        for it in vr.get('items', []):
            sn = it.get('snippet', {})
            if sn.get('liveBroadcastContent') in {'live', 'upcoming'}:
                continue
            det = it.get('contentDetails', {})
            dur_str, dur_s = iso2hms(det.get('duration', ''))
            st = it.get('status', {})
            stats = it.get('statistics', {})
            author = sn.get('channelTitle', '')
            avatar = amap.get(sn.get('channelId', ''), '')
            if sn.get('defaultAudioLanguage'):
                ls, lang = 'audio', sn['defaultAudioLanguage']
            elif sn.get('defaultLanguage'):
                ls, lang = 'default', sn['defaultLanguage']
            else:
                ls, lang = 'detect', safe_detect(sn.get('title', '') + ' ' + sn.get('description', '')) or 'unknown'
            lic = st.get('license', '')
            emb = st.get('embeddable', False)
            allowed = emb and lic == 'creativeCommon'
            rows.append([
                keyword, input_url, it['id'], sn.get('title', ''), sn.get('description', ''),
                lang, ls, 'short' if dur_s < 60 else 'video',
                dur_str, dur_s, fmt_age(sn.get('publishedAt', '')), author, avatar,
                stats.get('viewCount', ''), stats.get('likeCount', ''), stats.get('dislikeCount', ''),
                emb, lic, allowed,
                it.get('player', {}).get('embedHtml', f"<iframe src=\"https://www.youtube.com/embed/{it['id']}\" allowfullscreen></iframe>")
            ])
        token = sr.get('nextPageToken') or None
    return rows

# ─── Главная функция ───────────────────────────────────────────
r1 = sheets_service.spreadsheets().values().get(
    spreadsheetId=SHEET_ID,
    range=f"{KEYWORDS_SHEET}!A2:A"
).execute()
r2 = sheets_service.spreadsheets().values().get(
    spreadsheetId=SHEET_ID,
    range=f"{KEYWORDS_SHEET}!J2:J"
).execute()
keywords = [r[0] for r in r1.get('values', [])]
input_urls = [r[0] for r in r2.get('values', [])]

prog = shelve.open('progress.db')
start = prog.get('last_index', 0)
all_res = []
batch_res = []

try:
    for idx in range(start, len(keywords)):
        kw = keywords[idx]
        input_url = input_urls[idx] if idx < len(input_urls) else ''
        logger.info(f"🔍 [{idx}] {kw}")
        res = search_once(kw, input_url)

        all_res.extend(res)
        batch_res.extend(res)
        prog['last_index'] = idx + 1

        # Сохранение батча каждые BATCH_SIZE ключей
        if (idx + 1 - start) % BATCH_SIZE == 0:
            df_batch = pd.DataFrame(batch_res, columns=[
                'keyword','input_url','videoId','title','description','language','language_source',
                'video_type','duration','duration_seconds','age','author','author_avatar',
                'view_count','like_count','dislike_count','embeddable','license',
                'allowed_on_third_party','iframe'
            ])
            append_csv_batch(df_batch, OUTPUT_CSV)
            append_to_sheets_with_retry(
                sheets_service,
                SHEET_ID,
                f"{RESULTS_SHEET}!A2",
                batch_res
            )
            logger.info(f"🔄 Батч из {len(batch_res)} результатов сохранён после {(idx+1-start)} ключей")
            batch_res = []

except QuotaExceededAllKeys:
    logger.warning("⚠️ Все ключи исчерпали квоту, завершаем работу")
finally:
    prog.close()
    CACHE.close()

# Сохраняем остаток батча после завершения
if batch_res:
    df_batch = pd.DataFrame(batch_res, columns=[
        'keyword','input_url','videoId','title','description','language','language_source',
        'video_type','duration','duration_seconds','age','author','author_avatar',
        'view_count','like_count','dislike_count','embeddable','license',
        'allowed_on_third_party','iframe'
    ])
    append_csv_batch(df_batch, OUTPUT_CSV)
    append_to_sheets_with_retry(
        sheets_service,
        SHEET_ID,
        f"{RESULTS_SHEET}!A2",
        batch_res
    )
    logger.info(f"🔄 Финальный батч из {len(batch_res)} строк сохранён")

# Итоговый расход квоты
total_units = sum(api.used_units for api in key_manager.keys)
logger.info(f"ℹ️ Всего расход: {total_units} units")
