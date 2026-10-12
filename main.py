# import os
# import re
# import time
# import shutil
# import bisect
# import threading
# import psutil
# import pandas as pd
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extras import execute_values, Json
# from collections import Counter, deque
# from queue import Queue, Empty, Full
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from dotenv import load_dotenv
# from transformers import AutoTokenizer
# import fitz
# import libtorrent as lt

# fitz.TOOLS.mupdf_display_errors(False)
# fitz.TOOLS.mupdf_display_warnings(False)

# try:
#     os.set_blocking(1, True)
#     os.set_blocking(2, True)
# except (AttributeError, OSError):
#     pass

# load_dotenv()

# PG_DSN             = os.getenv("SUPABASE_PG_DSN")

# CHUNK_SIZE         = 512
# CHUNK_OVERLAP      = 50
# MD5_CSV_PATH       = "./md5.csv"
# DUMMY_TABLE        = "dummy_table"
# BASE_SAVE_PATH     = "./downloads"


# WORKER_FILES        = [f"part_{i}.txt" for i in range(1, 9)]
# MAGNETS_PER_WORKER  = 8
# MAX_PDF_EXTRACT     = 2
# MAX_PDF_SIZE_MB     = 60
# BATCH_SIZE          = 1000

# EXTRACT_QUEUE_MAXSIZE = 15

# MIN_FREE_DISK_GB   = 5
# DISK_CHECK_EVERY_S = 10

# TORRENT_DEADLINE_S   = 1800
# STALL_TIMEOUT_S      = 180
# REANNOUNCE_EVERY_S   = 30
# METADATA_TIMEOUT_S   = 90

# EXTRA_TRACKERS = [
#     "udp://tracker.opentrackr.org:1337/announce",
#     "udp://tracker.openbittorrent.com:6969/announce",
#     "udp://open.stealth.si:80/announce",
#     "udp://exodus.desync.com:6969/announce",
#     "udp://tracker.torrent.eu.org:451/announce",
#     "udp://tracker.tiny-vps.com:6969/announce",
#     "udp://tracker.dler.org:6969/announce",
#     "udp://explodie.org:6969/announce",
#     "udp://tracker.moeking.me:6969/announce",
#     "http://tracker.openbittorrent.com:80/announce",
# ]

# DHT_BOOTSTRAP_NODES = (
#     "router.bittorrent.com:6881,"
#     "router.utorrent.com:6881,"
#     "dht.transmissionbt.com:6881,"
#     "dht.aelitis.com:6881,"
#     "router.bitcomet.com:6881"
# )

# def build_session():
#     settings = {
#         "listen_interfaces":   "0.0.0.0:6881,[::]:6881",
#         "enable_dht":          True,
#         "enable_lsd":          True,
#         "enable_natpmp":       True,
#         "enable_upnp":         True,
#         "dht_bootstrap_nodes": DHT_BOOTSTRAP_NODES,
#         "connections_limit":   1500,
#         "unchoke_slots_limit": 60,
#         "active_downloads":    64,
#         "active_seeds":        16,
#         "active_limit":        128,
#         "download_rate_limit": 0,
#         "upload_rate_limit":   5 * 1024 * 1024,
#         "peer_connect_timeout":    7,
#         "request_queue_size":      500,
#         "max_out_request_queue":   1500,
#         "alert_mask":          lt.alert.category_t.all_categories,
#     }
#     while True:
#         try:
#             return lt.session(settings)
#         except KeyError as e:
#             msg = str(e)
#             bad_key = next((k for k in settings if k in msg), None)
#             if bad_key is None:
#                 raise
#             print(f"This libtorrent build doesn't support setting '{bad_key}' - skipping it.")
#             del settings[bad_key]

# CATEGORY_TABLE_MAP = {
#  "Anthropology":         "RAG_Anthropology_pg",
#  "Biography":            "RAG_Biography_pg",
#  "Business":             "RAG_Business_pg",
#  "Communication":        "RAG_Communication_pg",
#  "Criminology":          "RAG_Criminology_pg",
#  "CulturalStudies":      "RAG_CulturalStudies_pg",
#  "Economics":            "RAG_Economics_pg",
#  "Entrepreneurship":     "RAG_Entrepreneurship_pg",
#  "Film & Theatre":       "RAG_Film & Theatre_pg",
#  "Finance":              "RAG_Finance_pg",
#  "Geography":            "RAG_Geography_pg",
#  "Health":               "RAG_Health_pg",
#  "History":              "RAG_History_pg",
#  "Knowledge":            "RAG_Knowledge_pg",
#  "Law":                  "RAG_Law_pg",
#  "Neuroscience":         "RAG_Neuroscience_pg",
#  "Personal Development": "RAG_Personal Development_pg",
#  "Philosophy":           "RAG_Philosophy_pg",
#  "Politics":             "RAG_Politics_pg",
#  "Psychology":           "RAG_Psychology_pg",
#  "Religion":             "RAG_Religion_pg",
#  "Science":              "RAG_Science_pg",
#  "SelfHelp":             "RAG_SelfHelp_pg",
#  "SocialScience":        "RAG_SocialScience_pg",
#  "Sociology":            "RAG_Sociology_pg",
#  "Sports":               "RAG_Sports_pg",
#  "Technology":           "RAG_Technology_pg",
#  "Travel":               "RAG_Travel_pg",
#  "Astronomy":            "RAG_Astronomy_pg",
# }

# extract_queue = Queue(maxsize=EXTRACT_QUEUE_MAXSIZE)
# done_md5s     = set()
# done_lock     = threading.Lock()
# dead_letter   = []
# dead_lock     = threading.Lock()
# _print_lock   = threading.Lock()
# _extract_sem  = threading.Semaphore(MAX_PDF_EXTRACT)

# skipped_md5s  = set()
# skipped_lock  = threading.Lock()

# # ---------------------------------------------------------------------------
# # RETRY-SPEED FIX
# # ---------------------------------------------------------------------------
# # The retry-until-done driver re-adds EVERY magnet from scratch on every
# # pass -- including magnets that have nothing left to give (all their
# # wanted files are already ingested/permanently-skipped) and magnets that
# # just failed to find any peers last pass. Both cases pay the full
# # METADATA_TIMEOUT_S-scale cost again for zero benefit, which is most of
# # why passes feel slow to cycle. Two caches fix this:
# #
# # 1. exhausted_magnets: a magnet whose wanted files are ALL resolved
# #    (done or permanently skipped) gets cached here and is skipped
# #    entirely on future passes -- no metadata fetch at all.
# # 2. magnet_backoff / magnet_fail_streak: a magnet that fails to produce
# #    ANY successful download this pass (metadata timeout, or every wanted
# #    file stalled/timed out) gets an exponential cooldown before it's
# #    retried again, so your limited concurrent slots go to magnets that
# #    haven't been tried recently instead of re-hammering the same dead
# #    ones every single pass. A magnet resets its streak the moment it
# #    successfully yields a download.
# exhausted_magnets   = set()
# exhausted_lock      = threading.Lock()

# magnet_backoff      = {}   # magnet_uri -> epoch time it's next eligible for retry
# magnet_fail_streak  = {}   # magnet_uri -> consecutive passes with zero success
# backoff_lock        = threading.Lock()

# BASE_BACKOFF_S = 300     # 5 min after first failure
# MAX_BACKOFF_S  = 7200    # cap at 2 hours

# def _magnet_on_cooldown(magnet_uri):
#     with backoff_lock:
#         return time.time() < magnet_backoff.get(magnet_uri, 0)

# def _record_magnet_failure(magnet_uri):
#     with backoff_lock:
#         streak = magnet_fail_streak.get(magnet_uri, 0) + 1
#         magnet_fail_streak[magnet_uri] = streak
#         backoff = min(BASE_BACKOFF_S * (2 ** (streak - 1)), MAX_BACKOFF_S)
#         magnet_backoff[magnet_uri] = time.time() + backoff
#     return streak, backoff

# def _record_magnet_success(magnet_uri):
#     with backoff_lock:
#         magnet_fail_streak.pop(magnet_uri, None)
#         magnet_backoff.pop(magnet_uri, None)

# stats_lock          = threading.Lock()
# total_rows_inserted = 0
# last_ingested_md5   = None
# last_ingested_table = None
# last_ingested_rows  = 0
# last_ingested_at     = None

# session_done  = 0
# session_lock  = threading.Lock()

# start_total = time.time()

# ROLLING_WINDOW_S    = 900
# completion_times    = deque()
# completion_lock     = threading.Lock()

# def _record_completion():
#     now = time.time()
#     with completion_lock:
#         completion_times.append(now)
#         cutoff = now - ROLLING_WINDOW_S
#         while completion_times and completion_times[0] < cutoff:
#             completion_times.popleft()

# def _current_rate_per_hour():
#     now = time.time()
#     with completion_lock:
#         cutoff = now - ROLLING_WINDOW_S
#         while completion_times and completion_times[0] < cutoff:
#             completion_times.popleft()
#         n = len(completion_times)
#     return n * (3600.0 / ROLLING_WINDOW_S)

# def fmt(s):
#     h, r = divmod(int(s), 3600)
#     m, s = divmod(r, 60)
#     return f"{h}h {m}m {s}s"

# def log(worker_id, msg):
#     with _print_lock:
#         try:
#             print(f"[W{worker_id:02d}] {msg}", flush=True)
#         except Exception:
#             pass

# def ram_free_gb():
#     return psutil.virtual_memory().available / (1024 ** 3)

# def disk_free_gb(path="."):
#     try:
#         return shutil.disk_usage(path).free / (1024 ** 3)
#     except Exception:
#         return float("inf")

# def wait_for_disk_space(worker_id, tag, path=BASE_SAVE_PATH, min_gb=MIN_FREE_DISK_GB):
#     waited = False
#     while disk_free_gb(path) < min_gb:
#         if not waited:
#             log(worker_id, f"[{tag}] Low disk space (<{min_gb}GB free) - pausing downloads")
#             waited = True
#         time.sleep(DISK_CHECK_EVERY_S)
#     if waited:
#         log(worker_id, f"[{tag}] Disk space recovered - resuming downloads")

# def log_progress(worker_id):
#     with done_lock:
#         done_n = len(done_md5s)
#     with dead_lock:
#         dead_n = len(dead_letter)
#     with skipped_lock:
#         skipped_n = len(skipped_md5s)
#     with stats_lock:
#         rows_n    = total_rows_inserted
#         last_md5  = last_ingested_md5
#         last_tbl  = last_ingested_table
#     with session_lock:
#         sess_n = session_done
#     total     = len(wanted_md5_set)
#     resolved  = done_n + skipped_n
#     remaining = max(total - resolved, 0)
#     rate_hr   = _current_rate_per_hour()
#     last_str  = f"{last_md5} -> {last_tbl}" if last_md5 else "none yet"
#     log(worker_id,
#         f"[PROGRESS] This session ingested: {sess_n} books | "
#         f"{resolved}/{total} resolved ({done_n} ingested, {skipped_n} permanently skipped) | "
#         f"{dead_n} dead-letter entries pending retry - {remaining} remaining | "
#         f"Rows inserted: {rows_n:,} | Rate: {rate_hr:.0f}/hr (~{rate_hr*24:,.0f}/day) | "
#         f"Last ingested: {last_str}")

# _ROMAN_MAP = {
#     "i":1,"ii":2,"iii":3,"iv":4,"v":5,"vi":6,"vii":7,"viii":8,
#     "ix":9,"x":10,"xi":11,"xii":12,"xiii":13,"xiv":14,"xv":15,
#     "xvi":16,"xvii":17,"xviii":18,"xix":19,"xx":20,
#     "xxi":21,"xxii":22,"xxiii":23,"xxiv":24,"xxv":25,
# }
# _WORD_NUM_MAP = {
#     "one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,
#     "eight":8,"nine":9,"ten":10,"eleven":11,"twelve":12,"thirteen":13,
#     "fourteen":14,"fifteen":15,"sixteen":16,"seventeen":17,"eighteen":18,
#     "nineteen":19,"twenty":20,
# }

# def _normalise_number(raw):
#     s = raw.strip()
#     if re.match(r"^\d+$", s): return s
#     lo = s.lower()
#     if lo in _ROMAN_MAP:    return str(_ROMAN_MAP[lo])
#     if lo in _WORD_NUM_MAP: return str(_WORD_NUM_MAP[lo])
#     if len(s) == 1 and s.isalpha(): return s.upper()
#     return lo

# _NUM_TOK = (
#     r"(?:\d+|[ivxlcdmIVXLCDM]{1,10}"
#     r"|one|two|three|four|five|six|seven|eight"
#     r"|nine|ten|eleven|twelve|thirteen|fourteen"
#     r"|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
# )
# _NAME_TAIL  = r"(?:[\s]*[:\-\u2013\u2014\.][\s]*|\s+)(?P<name>[^\n]{1,120}?)"
# _RE_CHAPTER = re.compile(
#     rf"^\s*(?:chapter|chap\.?|ch\.?)\s+(?P<num>{_NUM_TOK})(?:{_NAME_TAIL})?\s*$", re.I)
# _RE_T3 = re.compile(r"^\s*(?P<major>\d+)(?:\.\d+)*\s+(?P<name>[A-Z][^\n]{0,120}?)\s*$")
# _T4_KEYWORDS = {
#     "preface","epilogue","bibliography","introduction","conclusion",
#     "appendix","foreword","abstract","acknowledgements","acknowledgments",
#     "glossary","index","references","contents","table of contents",
# }

# def _detect_chapter(line):
#     s = line.strip()
#     if not s or len(s) > 160 or s.isdigit(): return None
#     m = _RE_CHAPTER.match(s)
#     if m:
#         num  = _normalise_number(m.group("num"))
#         name = m.groupdict().get("name")
#         name = name.strip().strip(":.\u2013\u2014-").strip() if name else None
#         return "chapter", (f"Chapter {num}: {name}" if name else f"Chapter {num}")
#     m3 = _RE_T3.match(s)
#     if m3:
#         return "chapter", f"Chapter {m3.group('major')}: {m3.group('name').strip()}"
#     if s.lower().rstrip(":.") in _T4_KEYWORDS:
#         return "section", s.strip()
#     return None

# tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")

# def extract_book(pdf_path):
#     doc = fitz.open(pdf_path)
#     try:
#         first_line_counts = Counter()
#         for page in doc:
#             lines = [l.strip() for l in page.get_text().split("\n") if l.strip()]
#             if lines: first_line_counts[lines[0]] += 1
#         running_headers = {l for l, c in first_line_counts.items() if c >= 3}

#         full_ids, meta_spans = [], []
#         sec_type, sec_label = "body", None
#         for pno, page in enumerate(doc, 1):
#             text = page.get_text()
#             if not text.strip(): continue
#             lines = text.split("\n")
#             if lines and lines[0].strip() in running_headers:
#                 lines = lines[1:]
#             text = "\n".join(lines)
#             if not text.strip(): continue
#             for line in text.split("\n"):
#                 hit = _detect_chapter(line)
#                 if hit: sec_type, sec_label = hit
#             ids = tokenizer.encode(text, add_special_tokens=False)
#             if not ids:
#                 continue
#             meta_spans.append((len(full_ids), pno, sec_type, sec_label))
#             full_ids.extend(ids)
#         return full_ids, meta_spans
#     finally:
#         doc.close()

# def split_into_chunks(full_ids, meta_spans):
#     if not meta_spans:
#         return []
#     span_starts = [m[0] for m in meta_spans]
#     chunks, stride, i = [], CHUNK_SIZE - CHUNK_OVERLAP, 0
#     while i < len(full_ids):
#         ids = full_ids[i:i + CHUNK_SIZE]
#         if not ids: break
#         decoded = tokenizer.decode(ids, skip_special_tokens=True)
#         if len(decoded.strip()) >= 30:
#             span_idx = bisect.bisect_right(span_starts, i) - 1
#             span_idx = max(span_idx, 0)
#             _, page, sec_type, sec_label = meta_spans[span_idx]
#             chunks.append((decoded, sec_type, sec_label, page))
#         i += stride
#     return chunks

# _pg_local = threading.local()

# def get_pg_conn():
#     if not getattr(_pg_local, "conn", None) or _pg_local.conn.closed:
#         _pg_local.conn = psycopg2.connect(PG_DSN)
#         _pg_local.conn.autocommit = False
#     return _pg_local.conn

# def insert_chunks_pg(worker_id, md5, table_name, total_chunks, rows):
#     conn = get_pg_conn()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("BEGIN")
#             execute_values(
#                 cur,
#                 sql.SQL("""INSERT INTO {tbl}
#                     (content, md5, chunk_id, chunk_index)
#                     VALUES %s
#                     ON CONFLICT (chunk_id) DO NOTHING""").format(
#                         tbl=sql.Identifier(table_name)),
#                 rows,
#                 page_size=BATCH_SIZE,
#             )
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         log(worker_id, f"DB ERROR for {md5} -> {table_name}: {e}")
#         raise

# def _queue_for_extraction(worker_id, tag, final, md5, table):
#     while True:
#         try:
#             extract_queue.put((final, md5, table), timeout=DISK_CHECK_EVERY_S)
#             break
#         except Full:
#             log(worker_id, f"[{tag}] Extraction queue full - waiting for it to drain")
#             continue
#     log(worker_id, f"[{tag}] Queued for extraction: {md5} -> {table}")

# def download_worker(worker_id, magnet_uri, torrent_idx, ses, save_root):
#     tag      = f"DL-{worker_id}.{torrent_idx}"

#     # RETRY-SPEED FIX: nothing left to gain from this magnet -> don't even
#     # add the torrent / fetch metadata again.
#     with exhausted_lock:
#         if magnet_uri in exhausted_magnets:
#             return

#     # RETRY-SPEED FIX: this magnet failed recently -> let it cool down
#     # instead of re-attempting it (and paying full metadata-timeout cost)
#     # every single pass.
#     if _magnet_on_cooldown(magnet_uri):
#         return

#     save_dir = os.path.join(save_root, f"t{torrent_idx}")
#     os.makedirs(save_dir, exist_ok=True)

#     p = lt.parse_magnet_uri(magnet_uri)
#     p.save_path = save_dir
#     h = ses.add_torrent(p)

#     for url in EXTRA_TRACKERS:
#         try:
#             h.add_tracker({"url": url, "tier": 1})
#         except Exception:
#             pass

#     log(worker_id, f"[{tag}] Fetching metadata...")
#     last_nudge = 0.0
#     for i in range(METADATA_TIMEOUT_S):
#         if h.status().has_metadata:
#             break
#         now = time.time()
#         if now - last_nudge > REANNOUNCE_EVERY_S:
#             try:
#                 h.force_dht_announce()
#                 h.force_reannounce()
#             except Exception:
#                 pass
#             last_nudge = now
#         time.sleep(1)
#     else:
#         streak, backoff = _record_magnet_failure(magnet_uri)
#         log(worker_id, f"[{tag}] Metadata timeout (peers={h.status().num_peers}) - "
#                         f"skipping, cooling down {backoff:.0f}s (fail streak {streak})")
#         ses.remove_torrent(h)
#         return

#     tf   = h.torrent_file().files()
#     nf   = tf.num_files()
#     md5s = [tf.file_path(i).split("/")[1].upper() for i in range(nf)]

#     priorities = [0] * nf
#     wanted     = []
#     for i, md5 in enumerate(md5s):
#         if md5 not in wanted_md5_set:
#             continue
#         with done_lock:
#             if md5 in done_md5s:
#                 log(worker_id, f"[{tag}] Already ingested: {md5}")
#                 continue
#         category = md5_category.get(md5)
#         table = CATEGORY_TABLE_MAP.get(category)
#         if table is None:
#             log(worker_id, f"[{tag}] Unmapped/unknown category '{category}' for {md5} - skipping (permanent)")
#             with skipped_lock:
#                 skipped_md5s.add(md5)
#             continue
#         if tf.file_size(i) > MAX_PDF_SIZE_MB * 1024 * 1024:
#             log(worker_id, f"[{tag}] Skipping large file ({tf.file_size(i)//1024//1024}MB): {md5} (permanent)")
#             with skipped_lock:
#                 skipped_md5s.add(md5)
#             continue
#         priorities[i] = 1
#         wanted.append((i, md5, table))

#     if not wanted:
#         # RETRY-SPEED FIX: every file this magnet could ever offer us is
#         # already ingested or permanently skipped -- cache it so future
#         # passes don't fetch its metadata again at all.
#         log(worker_id, f"[{tag}] Nothing left to gain from this magnet - marking exhausted")
#         with exhausted_lock:
#             exhausted_magnets.add(magnet_uri)
#         ses.remove_torrent(h)
#         return

#     h.prioritize_files(priorities)
#     h.resume()
#     log(worker_id, f"[{tag}] {len(wanted)}/{nf} files queued (peers={h.status().num_peers})")

#     pending = {file_idx: (md5, table) for file_idx, md5, table in wanted}
#     last_bytes = {file_idx: 0 for file_idx in pending}
#     last_progress_at = {file_idx: time.time() for file_idx in pending}
#     last_nudge = 0.0
#     deadline = time.time() + TORRENT_DEADLINE_S
#     torrent_started_at = time.time()
#     got_any_download = False   # RETRY-SPEED FIX: did this attempt land anything?

#     while pending and time.time() < deadline:
#         wait_for_disk_space(worker_id, tag)

#         now = time.time()
#         status = h.status()
#         fp = h.file_progress()

#         if status.num_peers == 0 and now - last_nudge > REANNOUNCE_EVERY_S:
#             try:
#                 h.force_dht_announce()
#                 h.force_reannounce()
#             except Exception:
#                 pass
#             last_nudge = now

#         for file_idx in list(pending.keys()):
#             md5, table = pending[file_idx]
#             size = tf.file_size(file_idx)
#             done_bytes = fp[file_idx]

#             if done_bytes >= size:
#                 raw   = os.path.join(save_dir, tf.file_path(file_idx))
#                 final = raw + ".pdf"
#                 if os.path.exists(raw) and not os.path.exists(final):
#                     os.rename(raw, final)
#                 if os.path.exists(final):
#                     dl_time = time.time() - torrent_started_at
#                     log(worker_id, f"[{tag}] Downloaded -> {os.path.basename(final)} ({dl_time:.0f}s since torrent start)")
#                     _queue_for_extraction(worker_id, tag, final, md5, table)
#                     got_any_download = True
#                 else:
#                     log(worker_id, f"[{tag}] File missing after completion: {md5}")
#                 del pending[file_idx]
#                 continue

#             if done_bytes > last_bytes[file_idx]:
#                 last_bytes[file_idx] = done_bytes
#                 last_progress_at[file_idx] = now
#             elif now - last_progress_at[file_idx] > STALL_TIMEOUT_S:
#                 log(worker_id, f"[{tag}] Stalled ({STALL_TIMEOUT_S}s no progress, peers={status.num_peers}) - skipping {md5} (will retry next pass)")
#                 del pending[file_idx]

#         if pending:
#             time.sleep(2)

#     for file_idx, (md5, table) in pending.items():
#         log(worker_id, f"[{tag}] Timeout waiting for {md5} - skipping (will retry next pass)")

#     # RETRY-SPEED FIX: reset backoff on any success so a magnet that's
#     # actually working stays eligible every pass; otherwise back it off so
#     # we don't keep re-attempting a magnet that's producing nothing.
#     if got_any_download:
#         _record_magnet_success(magnet_uri)
#     else:
#         streak, backoff = _record_magnet_failure(magnet_uri)
#         log(worker_id, f"[{tag}] No successful downloads this attempt - "
#                         f"cooling down {backoff:.0f}s (fail streak {streak})")

#     ses.remove_torrent(h)
#     log(worker_id, f"[{tag}] Torrent done")

# def _delete_source_file(worker_id, md5, file_path):
#     try:
#         os.remove(file_path)
#         log(worker_id, f"[EX] Deleted source file for {md5}: {os.path.basename(file_path)}")
#     except FileNotFoundError:
#         pass
#     except Exception as e:
#         log(worker_id, f"[EX] Could not delete {file_path} for {md5}: {e}")

# def _extraction_worker_loop(worker_id, totals, t_lock, stop_event):
#     while not stop_event.is_set() or not extract_queue.empty():
#         try:
#             file_path, md5, table = extract_queue.get(timeout=5)
#         except Empty:
#             continue

#         try:
#             if not os.path.exists(file_path):
#                 log(worker_id, f"[EX] File gone: {file_path}")
#                 continue

#             pdf_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#             free_gb     = ram_free_gb()
#             if pdf_size_mb > 40 and free_gb < 2.0:
#                 log(worker_id, f"[EX] Low RAM ({free_gb:.1f}GB free), skipping {pdf_size_mb:.0f}MB PDF: {md5} (will retry next pass)")
#                 with dead_lock:
#                     dead_letter.append((file_path, md5, "low_ram"))
#                 _record_completion()
#                 log_progress(worker_id)
#                 continue

#             title = md5

#             t_extract = time.time()
#             with _extract_sem:
#                 try:
#                     full_ids, meta_spans = extract_book(file_path)
#                     chunks = split_into_chunks(full_ids, meta_spans)
#                 except Exception as e:
#                     log(worker_id, f"[EX] Extraction ERROR {md5}: {e} (will retry next pass)")
#                     with dead_lock:
#                         dead_letter.append((file_path, md5, str(e)))
#                     _record_completion()
#                     log_progress(worker_id)
#                     continue
#             extract_time = time.time() - t_extract

#             log(worker_id, f"[EX] {title}: {len(chunks)} chunks -> {table} (extract {extract_time:.1f}s, ram_free={ram_free_gb():.1f}GB)")

#             if not chunks:
#                 with done_lock:
#                     done_md5s.add(md5)
#                 global session_done
#                 with session_lock:
#                     session_done += 1
#                 _delete_source_file(worker_id, md5, file_path)
#                 _record_completion()
#                 log_progress(worker_id)
#                 continue

#             rows = [
#                 (
#                     chunk,
#                     md5,
#                     f"{md5[:6]}_{ci:04d}",
#                     ci,
#                 )
#                 for ci, (chunk, st, sl, pg) in enumerate(chunks)
#             ]

#             t_insert = time.time()
#             try:
#                 insert_chunks_pg(worker_id, md5, table, len(chunks), rows)
#                 insert_time = time.time() - t_insert
#                 log(worker_id, f"[EX] Inserted {len(rows)} rows for {md5} into {table} (insert {insert_time:.1f}s)")
#                 with done_lock:
#                     done_md5s.add(md5)
#                 with session_lock:
#                     session_done += 1
#                 global total_rows_inserted, last_ingested_md5, last_ingested_table, last_ingested_rows, last_ingested_at
#                 with stats_lock:
#                     total_rows_inserted += len(rows)
#                     last_ingested_md5   = md5
#                     last_ingested_table = table
#                     last_ingested_rows  = len(rows)
#                     last_ingested_at    = time.time()
#                 _record_completion()
#                 log_progress(worker_id)
#             except Exception as e:
#                 log(worker_id, f"[EX] Insert failed for {md5}: {e} - adding to dead letter (will retry next pass)")
#                 with dead_lock:
#                     dead_letter.append((file_path, md5, str(e)))
#                 _record_completion()
#                 log_progress(worker_id)
#                 continue

#             _delete_source_file(worker_id, md5, file_path)

#             with t_lock:
#                 totals["chunking"] += extract_time
#                 totals["saving"]   += insert_time

#         except Exception as e:
#             log(worker_id, f"[EX] UNEXPECTED ERROR on {md5}: {e} - adding to dead letter (will retry next pass)")
#             with dead_lock:
#                 dead_letter.append((file_path, md5, f"unexpected: {e}"))
#             _record_completion()
#             log_progress(worker_id)
#         finally:
#             extract_queue.task_done()


# def extraction_worker(worker_id, totals, t_lock, stop_event):
#     log(worker_id, "[EX] Extraction worker started")
#     while True:
#         try:
#             _extraction_worker_loop(worker_id, totals, t_lock, stop_event)
#             break
#         except Exception as e:
#             log(worker_id, f"[EX] Worker loop crashed, restarting: {e}")
#             if stop_event.is_set() and extract_queue.empty():
#                 break
#     log(worker_id, "[EX] Extraction worker stopped")

# def run_download_worker(worker_id, txt_file, totals, t_lock, ses):
#     if not os.path.exists(txt_file):
#         log(worker_id, f"File not found: {txt_file} - skipping")
#         return

#     with open(txt_file) as f:
#         magnets = [l.strip() for l in f if l.strip().startswith("magnet:")]

#     if not magnets:
#         log(worker_id, f"No magnet links in {txt_file} - skipping")
#         return

#     log(worker_id, f"Loaded {len(magnets)} magnet links")

#     save_root = os.path.join(BASE_SAVE_PATH, f"worker_{worker_id}")
#     os.makedirs(save_root, exist_ok=True)

#     t_dl = time.time()
#     with ThreadPoolExecutor(max_workers=MAGNETS_PER_WORKER) as ex:
#         futs = {
#             ex.submit(download_worker, worker_id, uri, idx, ses, save_root): (idx, uri)
#             for idx, uri in enumerate(magnets)
#         }
#         for fut in as_completed(futs):
#             idx, uri = futs[fut]
#             try:
#                 fut.result()
#             except Exception as e:
#                 log(worker_id, f"ERROR torrent {idx}: {e}")

#     with t_lock:
#         totals["download"] += time.time() - t_dl

#     log(worker_id, "Download stage done")

# if not PG_DSN:
#     raise RuntimeError(
#         "SUPABASE_PG_DSN not set.\n"
#         "Add to your .env:\n"
#         "  SUPABASE_PG_DSN=postgresql://postgres.xncfghdikiqknuruurfh:[PASSWORD]@aws-0-ap-south-1.pooler.supabase.com:5432/postgres"
#     )

# if not os.path.exists(MD5_CSV_PATH):
#     raise RuntimeError(f"{MD5_CSV_PATH} not found - put your md5.csv next to this script.")

# print("Loading wanted md5 list from CSV...")
# md5_df = pd.read_csv(MD5_CSV_PATH, dtype=str)
# md5_df["md5"] = md5_df["md5"].str.strip().str.upper()
# md5_df["category"] = md5_df["category"].str.strip()
# md5_category   = dict(zip(md5_df["md5"], md5_df["category"]))
# wanted_md5_set = set(md5_category.keys())
# print(f"Loaded {len(wanted_md5_set):,} wanted md5s.\n")

# unknown_categories = sorted(set(md5_category.values()) - set(CATEGORY_TABLE_MAP.keys()))
# if unknown_categories:
#     print(f"WARNING: these categories in md5.csv have no matching table and will be skipped: {unknown_categories}\n")

# print(f"Starting fresh - not checking {DUMMY_TABLE}. All {len(wanted_md5_set):,} md5s in md5.csv will be attempted.\n")

# totals = {"download": 0.0, "chunking": 0.0, "saving": 0.0}
# t_lock = threading.Lock()

# stop_event = threading.Event()

# def periodic_progress_logger(interval=15):
#     total = len(wanted_md5_set)
#     while not stop_event.is_set():
#         with done_lock:
#             done_n = len(done_md5s)
#         with dead_lock:
#             dead_n = len(dead_letter)
#         with skipped_lock:
#             skipped_n = len(skipped_md5s)
#         with stats_lock:
#             rows_n   = total_rows_inserted
#             last_md5 = last_ingested_md5
#             last_tbl = last_ingested_table
#             last_rows = last_ingested_rows
#             last_at  = last_ingested_at
#         with session_lock:
#             sess_n = session_done
#         resolved  = done_n + skipped_n
#         remaining = max(total - resolved, 0)
#         free_gb   = disk_free_gb(BASE_SAVE_PATH)
#         rate_hr   = _current_rate_per_hour()
#         with exhausted_lock:
#             exhausted_n = len(exhausted_magnets)
#         with backoff_lock:
#             cooling_n = sum(1 for t in magnet_backoff.values() if t > time.time())

#         if last_md5:
#             ago = f"{int(time.time() - last_at)}s ago" if last_at else "just now"
#             last_str = f"{last_md5} -> {last_tbl} ({last_rows} rows, {ago})"
#         else:
#             last_str = "none yet"

#         log(0,
#             f"[PROGRESS] This session ingested: {sess_n} books | "
#             f"Total books: {total} | Ingested: {done_n} | Permanently skipped: {skipped_n} | "
#             f"Dead-letter (pending retry): {dead_n} | Remaining: {remaining} | "
#             f"Rows inserted: {rows_n:,} | "
#             f"Rate: {rate_hr:.0f}/hr (~{rate_hr*24:,.0f}/day, last 15min) | "
#             f"Last ingested: {last_str} | Disk free: {free_gb:.1f}GB | RAM free: {ram_free_gb():.1f}GB | "
#             f"Extract queue: {extract_queue.qsize()}/{EXTRACT_QUEUE_MAXSIZE} | "
#             f"Magnets exhausted: {exhausted_n:,} | Magnets cooling down: {cooling_n:,}")
#         stop_event.wait(interval)

# progress_thread = threading.Thread(target=periodic_progress_logger, daemon=True)
# progress_thread.start()

# os.makedirs(BASE_SAVE_PATH, exist_ok=True)

# if disk_free_gb(BASE_SAVE_PATH) < MIN_FREE_DISK_GB:
#     print(f"WARNING: only {disk_free_gb(BASE_SAVE_PATH):.1f}GB free at startup "
#           f"(threshold {MIN_FREE_DISK_GB}GB) - downloads will pause immediately until space frees up.\n")

# extraction_threads = []
# for i in range(len(WORKER_FILES)):
#     t = threading.Thread(
#         target=extraction_worker,
#         args=(i + 1, totals, t_lock, stop_event),
#         daemon=True,
#     )
#     t.start()
#     extraction_threads.append(t)

# print(f"Started {len(extraction_threads)} extraction workers.\n")

# print("Starting shared libtorrent session (DHT/LSD/trackers)...")
# GLOBAL_SES = build_session()
# _dht_wait_deadline = time.time() + 15
# while time.time() < _dht_wait_deadline:
#     if GLOBAL_SES.status().dht_nodes > 0:
#         break
#     time.sleep(1)
# print(f"DHT nodes in routing table: {GLOBAL_SES.status().dht_nodes}\n")

# NO_PROGRESS_BACKOFF_S = 60
# pass_num = 0
# while True:
#     pass_num += 1
#     with done_lock:
#         done_n = len(done_md5s)
#     with skipped_lock:
#         skipped_n = len(skipped_md5s)
#     remaining_before = len(wanted_md5_set) - done_n - skipped_n

#     if remaining_before <= 0:
#         print(f"\nAll {len(wanted_md5_set):,} books accounted for "
#               f"({done_n:,} ingested, {skipped_n:,} permanently un-ingestable). Stopping.\n")
#         break

#     print(f"\n{'-'*60}\n"
#           f"Pass {pass_num}: {remaining_before:,} book(s) still need ingesting - "
#           f"starting {len(WORKER_FILES)} download workers...\n"
#           f"{'-'*60}\n")

#     with ThreadPoolExecutor(max_workers=len(WORKER_FILES)) as ex:
#         futs = {
#             ex.submit(run_download_worker, i + 1, txt, totals, t_lock, GLOBAL_SES): txt
#             for i, txt in enumerate(WORKER_FILES)
#         }
#         for fut in as_completed(futs):
#             txt = futs[fut]
#             try:
#                 fut.result()
#             except Exception as e:
#                 with _print_lock:
#                     print(f"[ERROR] Download worker for {txt}: {e}", flush=True)

#     print(f"\nPass {pass_num}: downloads complete. Waiting for extraction queue to drain...")
#     extract_queue.join()

#     with done_lock:
#         done_n = len(done_md5s)
#     with skipped_lock:
#         skipped_n = len(skipped_md5s)
#     remaining_after = len(wanted_md5_set) - done_n - skipped_n
#     print(f"Pass {pass_num} complete - {remaining_after:,} book(s) still remaining.\n")

#     if remaining_after <= 0:
#         continue 

#     if remaining_after >= remaining_before:
#         log(0, f"No progress in pass {pass_num} ({remaining_after:,} still remaining) - "
#                 f"these are likely books not present in any of the current torrents, or "
#                 f"a persistent failure. Will keep retrying (not stopping until all books "
#                 f"are ingested) - backing off {NO_PROGRESS_BACKOFF_S}s before the next pass.")
#         time.sleep(NO_PROGRESS_BACKOFF_S)

# print("\nAll passes complete. Shutting down extraction workers...")
# stop_event.set()
# for t in extraction_threads:
#     t.join(timeout=30)

# with done_lock:
#     final_done_n = len(done_md5s)
# with skipped_lock:
#     final_skipped_n = len(skipped_md5s)
# final_remaining = len(wanted_md5_set) - final_done_n - final_skipped_n

# print(f"\n{'='*40}")
# print(f"All workers finished.")
# print(f"  This session ingested: {session_done:,} books")
# print(f"  Download:   {fmt(totals['download'])}")
# print(f"  Chunking:   {fmt(totals['chunking'])}")
# print(f"  DB insert:  {fmt(totals['saving'])}")
# print(f"  Wall time:  {fmt(time.time() - start_total)}")
# print(f"  Ingested (all-time): {final_done_n:,} / {len(wanted_md5_set):,} books")
# print(f"  Permanently skipped (unmapped category / oversized): {final_skipped_n:,}")
# print(f"  Still remaining (should be 0): {final_remaining:,}")
# print(f"  Rows:       {total_rows_inserted:,} chunk rows")
# print(f"  Last md5:   {last_ingested_md5 or 'none'} -> {last_ingested_table or '-'}")
# if dead_letter:
#     print(f"\n  Dead-letter log ({len(dead_letter)} entries across all passes - "
#           f"these were auto-retried on the next pass, this is just a history):")
#     for fp, md5, reason in dead_letter[:20]:
#         print(f"    {md5}: {reason}")
#     if len(dead_letter) > 20:
#         print(f"    ... and {len(dead_letter)-20} more")
# print(f"{'='*40}\n")




"""
DOWNLOADER - runs on VPS #2. Standalone file, no other local files needed.

Receives matched (magnet_uri, file_index, md5, table) jobs from the
Matcher (VPS #1) via FastAPI, does the real torrent download for just
those files, extracts text, chunks, and inserts into Postgres.

Install:  pip install libtorrent fastapi uvicorn psycopg2-binary \
                      transformers pymupdf psutil python-dotenv torch

.env (next to this file):
    SUPABASE_PG_DSN    = postgresql://...
    PORT               = 8000
    DOWNLOAD_WORKERS   = 16
    EXTRACTION_WORKERS = 8
    BASE_SAVE_PATH     = ./downloads
"""
# import os
# import re
# import time
# import bisect
# import shutil
# import threading
# from collections import Counter, deque
# from queue import Queue, Empty, Full
# from typing import List

# import psutil
# import psycopg2
# from psycopg2 import sql
# from psycopg2.extras import execute_values
# from dotenv import load_dotenv
# from transformers import AutoTokenizer
# import fitz
# import libtorrent as lt
# import uvicorn
# from fastapi import FastAPI
# from pydantic import BaseModel

# fitz.TOOLS.mupdf_display_errors(False)
# fitz.TOOLS.mupdf_display_warnings(False)

# load_dotenv()

# # ---------------------------------------------------------------------------
# # Config
# # ---------------------------------------------------------------------------
# PG_DSN               = os.getenv("SUPABASE_PG_DSN")
# PORT                  = int(os.getenv("PORT", "8000"))
# DOWNLOAD_WORKERS       = int(os.getenv("DOWNLOAD_WORKERS", "16"))
# EXTRACTION_WORKERS     = int(os.getenv("EXTRACTION_WORKERS", "8"))
# BASE_SAVE_PATH         = os.getenv("BASE_SAVE_PATH", "./downloads")

# CHUNK_SIZE            = 512
# CHUNK_OVERLAP         = 50
# BATCH_SIZE            = 1000
# MAX_PDF_EXTRACT       = 2
# JOB_QUEUE_MAXSIZE     = 500
# EXTRACT_QUEUE_MAXSIZE = 30
# MIN_FREE_DISK_GB      = 5
# DISK_CHECK_EVERY_S    = 10
# METADATA_TIMEOUT_S    = 90
# REANNOUNCE_EVERY_S    = 30
# TORRENT_DEADLINE_S    = 1800
# STALL_TIMEOUT_S       = 180

# EXTRA_TRACKERS = [
#     "udp://tracker.opentrackr.org:1337/announce",
#     "udp://tracker.openbittorrent.com:6969/announce",
#     "udp://open.stealth.si:80/announce",
#     "udp://exodus.desync.com:6969/announce",
#     "udp://tracker.torrent.eu.org:451/announce",
#     "udp://tracker.tiny-vps.com:6969/announce",
#     "udp://tracker.dler.org:6969/announce",
#     "udp://explodie.org:6969/announce",
#     "udp://tracker.moeking.me:6969/announce",
#     "http://tracker.openbittorrent.com:80/announce",
# ]

# DHT_BOOTSTRAP_NODES = (
#     "router.bittorrent.com:6881,"
#     "router.utorrent.com:6881,"
#     "dht.transmissionbt.com:6881,"
#     "dht.aelitis.com:6881,"
#     "router.bitcomet.com:6881"
# )

# if not PG_DSN:
#     raise RuntimeError(
#         "SUPABASE_PG_DSN not set.\nAdd to your .env:\n"
#         "  SUPABASE_PG_DSN=postgresql://...\n"
#     )

# os.makedirs(BASE_SAVE_PATH, exist_ok=True)

# _print_lock = threading.Lock()


# def log(worker_id, msg):
#     with _print_lock:
#         try:
#             print(f"[W{worker_id:02d}] {msg}", flush=True)
#         except Exception:
#             pass


# def build_session():
#     """Self-healing: drops any settings_pack key this libtorrent build
#     doesn't recognize instead of crashing on it."""
#     settings = {
#         "listen_interfaces":   "0.0.0.0:6881,[::]:6881",
#         "enable_dht":          True,
#         "enable_lsd":          True,
#         "enable_natpmp":       True,
#         "enable_upnp":         True,
#         "dht_bootstrap_nodes": DHT_BOOTSTRAP_NODES,
#         "connections_limit":   1000,
#         "unchoke_slots_limit": 60,
#         "active_downloads":    64,
#         "active_seeds":        16,
#         "active_limit":        128,
#         "download_rate_limit": 0,
#         "upload_rate_limit":   5 * 1024 * 1024,
#         "peer_connect_timeout":    7,
#         "request_queue_size":      500,
#         "max_out_request_queue":   1500,
#         "alert_mask":          lt.alert.category_t.all_categories,
#     }
#     while True:
#         try:
#             return lt.session(settings)
#         except KeyError as e:
#             msg = str(e)
#             bad_key = next((k for k in settings if k in msg), None)
#             if bad_key is None:
#                 raise
#             print(f"This libtorrent build doesn't support setting '{bad_key}' - skipping it.")
#             del settings[bad_key]


# # ---------------------------------------------------------------------------
# # State
# # ---------------------------------------------------------------------------
# job_queue     = Queue(maxsize=JOB_QUEUE_MAXSIZE)
# extract_queue = Queue(maxsize=EXTRACT_QUEUE_MAXSIZE)

# done_md5s      = set()
# done_lock      = threading.Lock()

# # Reserved the instant a job is accepted, released the instant that md5's
# # processing reaches a terminal state. Lets job intake dedupe against work
# # already in progress, so the matcher can re-send the same match every
# # pass without causing duplicate downloads.
# in_flight_md5s = set()
# in_flight_lock = threading.Lock()

# dead_letter    = []
# dead_lock      = threading.Lock()

# stats_lock          = threading.Lock()
# total_rows_inserted = 0
# last_ingested_md5    = None
# last_ingested_table  = None

# _extract_sem = threading.Semaphore(MAX_PDF_EXTRACT)

# ROLLING_WINDOW_S = 900
# completion_times = deque()
# completion_lock  = threading.Lock()


# def _record_completion():
#     now = time.time()
#     with completion_lock:
#         completion_times.append(now)
#         cutoff = now - ROLLING_WINDOW_S
#         while completion_times and completion_times[0] < cutoff:
#             completion_times.popleft()


# def _current_rate_per_hour():
#     now = time.time()
#     with completion_lock:
#         cutoff = now - ROLLING_WINDOW_S
#         while completion_times and completion_times[0] < cutoff:
#             completion_times.popleft()
#         n = len(completion_times)
#     return n * (3600.0 / ROLLING_WINDOW_S)


# def ram_free_gb():
#     return psutil.virtual_memory().available / (1024 ** 3)


# def disk_free_gb(path="."):
#     try:
#         return shutil.disk_usage(path).free / (1024 ** 3)
#     except Exception:
#         return float("inf")


# def wait_for_disk_space(worker_id, tag, path=BASE_SAVE_PATH, min_gb=MIN_FREE_DISK_GB):
#     waited = False
#     while disk_free_gb(path) < min_gb:
#         if not waited:
#             log(worker_id, f"[{tag}] Low disk space (<{min_gb}GB free) - pausing")
#             waited = True
#         time.sleep(DISK_CHECK_EVERY_S)
#     if waited:
#         log(worker_id, f"[{tag}] Disk space recovered")


# # ---------------------------------------------------------------------------
# # Extraction
# # ---------------------------------------------------------------------------
# _ROMAN_MAP = {
#     "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6, "vii": 7, "viii": 8,
#     "ix": 9, "x": 10, "xi": 11, "xii": 12, "xiii": 13, "xiv": 14, "xv": 15,
#     "xvi": 16, "xvii": 17, "xviii": 18, "xix": 19, "xx": 20,
#     "xxi": 21, "xxii": 22, "xxiii": 23, "xxiv": 24, "xxv": 25,
# }
# _WORD_NUM_MAP = {
#     "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7,
#     "eight": 8, "nine": 9, "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13,
#     "fourteen": 14, "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18,
#     "nineteen": 19, "twenty": 20,
# }


# def _normalise_number(raw):
#     s = raw.strip()
#     if re.match(r"^\d+$", s):
#         return s
#     lo = s.lower()
#     if lo in _ROMAN_MAP:
#         return str(_ROMAN_MAP[lo])
#     if lo in _WORD_NUM_MAP:
#         return str(_WORD_NUM_MAP[lo])
#     if len(s) == 1 and s.isalpha():
#         return s.upper()
#     return lo


# _NUM_TOK = (
#     r"(?:\d+|[ivxlcdmIVXLCDM]{1,10}"
#     r"|one|two|three|four|five|six|seven|eight"
#     r"|nine|ten|eleven|twelve|thirteen|fourteen"
#     r"|fifteen|sixteen|seventeen|eighteen|nineteen|twenty)"
# )
# _NAME_TAIL = r"(?:[\s]*[:\-\u2013\u2014\.][\s]*|\s+)(?P<name>[^\n]{1,120}?)"
# _RE_CHAPTER = re.compile(
#     rf"^\s*(?:chapter|chap\.?|ch\.?)\s+(?P<num>{_NUM_TOK})(?:{_NAME_TAIL})?\s*$", re.I)
# _RE_T3 = re.compile(r"^\s*(?P<major>\d+)(?:\.\d+)*\s+(?P<name>[A-Z][^\n]{0,120}?)\s*$")
# _T4_KEYWORDS = {
#     "preface", "epilogue", "bibliography", "introduction", "conclusion",
#     "appendix", "foreword", "abstract", "acknowledgements", "acknowledgments",
#     "glossary", "index", "references", "contents", "table of contents",
# }


# def _detect_chapter(line):
#     s = line.strip()
#     if not s or len(s) > 160 or s.isdigit():
#         return None
#     m = _RE_CHAPTER.match(s)
#     if m:
#         num = _normalise_number(m.group("num"))
#         name = m.groupdict().get("name")
#         name = name.strip().strip(":.\u2013\u2014-").strip() if name else None
#         return "chapter", (f"Chapter {num}: {name}" if name else f"Chapter {num}")
#     m3 = _RE_T3.match(s)
#     if m3:
#         return "chapter", f"Chapter {m3.group('major')}: {m3.group('name').strip()}"
#     if s.lower().rstrip(":.") in _T4_KEYWORDS:
#         return "section", s.strip()
#     return None


# tokenizer = AutoTokenizer.from_pretrained("BAAI/bge-m3")


# def extract_book(pdf_path):
#     doc = fitz.open(pdf_path)
#     try:
#         first_line_counts = Counter()
#         for page in doc:
#             lines = [l.strip() for l in page.get_text().split("\n") if l.strip()]
#             if lines:
#                 first_line_counts[lines[0]] += 1
#         running_headers = {l for l, c in first_line_counts.items() if c >= 3}

#         full_ids, meta_spans = [], []
#         sec_type, sec_label = "body", None
#         for pno, page in enumerate(doc, 1):
#             text = page.get_text()
#             if not text.strip():
#                 continue
#             lines = text.split("\n")
#             if lines and lines[0].strip() in running_headers:
#                 lines = lines[1:]
#             text = "\n".join(lines)
#             if not text.strip():
#                 continue
#             for line in text.split("\n"):
#                 hit = _detect_chapter(line)
#                 if hit:
#                     sec_type, sec_label = hit
#             ids = tokenizer.encode(text, add_special_tokens=False)
#             if not ids:
#                 continue
#             meta_spans.append((len(full_ids), pno, sec_type, sec_label))
#             full_ids.extend(ids)
#         return full_ids, meta_spans
#     finally:
#         doc.close()


# def split_into_chunks(full_ids, meta_spans):
#     if not meta_spans:
#         return []
#     span_starts = [m[0] for m in meta_spans]
#     chunks, stride, i = [], CHUNK_SIZE - CHUNK_OVERLAP, 0
#     while i < len(full_ids):
#         ids = full_ids[i:i + CHUNK_SIZE]
#         if not ids:
#             break
#         decoded = tokenizer.decode(ids, skip_special_tokens=True)
#         if len(decoded.strip()) >= 30:
#             span_idx = bisect.bisect_right(span_starts, i) - 1
#             span_idx = max(span_idx, 0)
#             _, page, sec_type, sec_label = meta_spans[span_idx]
#             chunks.append((decoded, sec_type, sec_label, page))
#         i += stride
#     return chunks


# _pg_local = threading.local()


# def get_pg_conn():
#     if not getattr(_pg_local, "conn", None) or _pg_local.conn.closed:
#         _pg_local.conn = psycopg2.connect(PG_DSN)
#         _pg_local.conn.autocommit = False
#     return _pg_local.conn


# def insert_chunks_pg(worker_id, md5, table_name, rows):
#     conn = get_pg_conn()
#     try:
#         with conn.cursor() as cur:
#             cur.execute("BEGIN")
#             execute_values(
#                 cur,
#                 sql.SQL("""INSERT INTO {tbl}
#                     (content, md5, chunk_id, chunk_index)
#                     VALUES %s
#                     ON CONFLICT (chunk_id) DO NOTHING""").format(
#                     tbl=sql.Identifier(table_name)),
#                 rows,
#                 page_size=BATCH_SIZE,
#             )
#         conn.commit()
#     except Exception as e:
#         conn.rollback()
#         log(worker_id, f"DB ERROR for {md5} -> {table_name}: {e}")
#         raise


# def _delete_source_file(worker_id, md5, file_path):
#     try:
#         os.remove(file_path)
#     except FileNotFoundError:
#         pass
#     except Exception as e:
#         log(worker_id, f"[EX] Could not delete {file_path} for {md5}: {e}")


# def _release_in_flight(md5):
#     with in_flight_lock:
#         in_flight_md5s.discard(md5)


# def _queue_for_extraction(worker_id, tag, final, md5, table):
#     while True:
#         try:
#             extract_queue.put((final, md5, table), timeout=DISK_CHECK_EVERY_S)
#             break
#         except Full:
#             log(worker_id, f"[{tag}] Extraction queue full - waiting")
#             continue
#     log(worker_id, f"[{tag}] Queued for extraction: {md5} -> {table}")


# def extraction_worker_loop(worker_id):
#     while True:
#         try:
#             file_path, md5, table = extract_queue.get(timeout=5)
#         except Empty:
#             continue

#         try:
#             if not os.path.exists(file_path):
#                 log(worker_id, f"[EX] File gone: {file_path}")
#                 _release_in_flight(md5)
#                 continue

#             pdf_size_mb = os.path.getsize(file_path) / (1024 * 1024)
#             free_gb = ram_free_gb()
#             if pdf_size_mb > 40 and free_gb < 2.0:
#                 log(worker_id, f"[EX] Low RAM ({free_gb:.1f}GB free), skipping {pdf_size_mb:.0f}MB PDF: {md5}")
#                 with dead_lock:
#                     dead_letter.append((file_path, md5, "low_ram"))
#                 _release_in_flight(md5)
#                 _record_completion()
#                 continue

#             t_extract = time.time()
#             with _extract_sem:
#                 try:
#                     full_ids, meta_spans = extract_book(file_path)
#                     chunks = split_into_chunks(full_ids, meta_spans)
#                 except Exception as e:
#                     log(worker_id, f"[EX] Extraction ERROR {md5}: {e}")
#                     with dead_lock:
#                         dead_letter.append((file_path, md5, str(e)))
#                     _release_in_flight(md5)
#                     _record_completion()
#                     continue
#             extract_time = time.time() - t_extract
#             log(worker_id, f"[EX] {md5}: {len(chunks)} chunks -> {table} (extract {extract_time:.1f}s)")

#             if not chunks:
#                 with done_lock:
#                     done_md5s.add(md5)
#                 _delete_source_file(worker_id, md5, file_path)
#                 _release_in_flight(md5)
#                 _record_completion()
#                 continue

#             rows = [
#                 (chunk, md5, f"{md5[:6]}_{ci:04d}", ci)
#                 for ci, (chunk, st, sl, pg) in enumerate(chunks)
#             ]

#             t_insert = time.time()
#             try:
#                 insert_chunks_pg(worker_id, md5, table, rows)
#                 insert_time = time.time() - t_insert
#                 log(worker_id, f"[EX] Inserted {len(rows)} rows for {md5} into {table} (insert {insert_time:.1f}s)")
#                 with done_lock:
#                     done_md5s.add(md5)
#                 global total_rows_inserted, last_ingested_md5, last_ingested_table
#                 with stats_lock:
#                     total_rows_inserted += len(rows)
#                     last_ingested_md5 = md5
#                     last_ingested_table = table
#                 _release_in_flight(md5)
#                 _record_completion()
#             except Exception as e:
#                 log(worker_id, f"[EX] Insert failed for {md5}: {e}")
#                 with dead_lock:
#                     dead_letter.append((file_path, md5, str(e)))
#                 _release_in_flight(md5)
#                 _record_completion()
#                 continue

#             _delete_source_file(worker_id, md5, file_path)

#         except Exception as e:
#             log(worker_id, f"[EX] UNEXPECTED ERROR on {md5}: {e}")
#             with dead_lock:
#                 dead_letter.append((file_path, md5, f"unexpected: {e}"))
#             _release_in_flight(md5)
#             _record_completion()
#         finally:
#             extract_queue.task_done()


# # ---------------------------------------------------------------------------
# # Download
# # ---------------------------------------------------------------------------
# def process_job(worker_id, job, ses):
#     magnet_uri = job["magnet_uri"]
#     files = job["files"]
#     tag = f"DL-{worker_id}"

#     p = lt.parse_magnet_uri(magnet_uri)
#     save_dir = os.path.join(BASE_SAVE_PATH, f"job_{worker_id}_{int(time.time() * 1000)}")
#     os.makedirs(save_dir, exist_ok=True)
#     p.save_path = save_dir
#     h = ses.add_torrent(p)

#     for url in EXTRA_TRACKERS:
#         try:
#             h.add_tracker({"url": url, "tier": 1})
#         except Exception:
#             pass

#     log(worker_id, f"[{tag}] Fetching metadata for {len(files)} file(s)...")
#     last_nudge = 0.0
#     for i in range(METADATA_TIMEOUT_S):
#         if h.status().has_metadata:
#             break
#         now = time.time()
#         if now - last_nudge > REANNOUNCE_EVERY_S:
#             try:
#                 h.force_dht_announce()
#                 h.force_reannounce()
#             except Exception:
#                 pass
#             last_nudge = now
#         time.sleep(1)
#     else:
#         log(worker_id, f"[{tag}] Metadata timeout (peers={h.status().num_peers}) - releasing job")
#         for f in files:
#             _release_in_flight(f["md5"])
#         ses.remove_torrent(h)
#         return

#     tf = h.torrent_file().files()
#     nf = tf.num_files()

#     priorities = [0] * nf
#     pending = {}
#     for f in files:
#         idx = f["file_index"]
#         if idx < 0 or idx >= nf:
#             log(worker_id, f"[{tag}] Bad file_index {idx} for {f['md5']} - releasing")
#             _release_in_flight(f["md5"])
#             continue
#         priorities[idx] = 1
#         pending[idx] = (f["md5"], f["table"])

#     if not pending:
#         ses.remove_torrent(h)
#         return

#     h.prioritize_files(priorities)
#     h.resume()
#     log(worker_id, f"[{tag}] Downloading {len(pending)} file(s) (peers={h.status().num_peers})")

#     last_bytes = {i: 0 for i in pending}
#     last_progress_at = {i: time.time() for i in pending}
#     last_nudge = 0.0
#     deadline = time.time() + TORRENT_DEADLINE_S

#     while pending and time.time() < deadline:
#         wait_for_disk_space(worker_id, tag)

#         now = time.time()
#         status = h.status()
#         fp = h.file_progress()

#         if status.num_peers == 0 and now - last_nudge > REANNOUNCE_EVERY_S:
#             try:
#                 h.force_dht_announce()
#                 h.force_reannounce()
#             except Exception:
#                 pass
#             last_nudge = now

#         for file_idx in list(pending.keys()):
#             md5, table = pending[file_idx]
#             size = tf.file_size(file_idx)
#             done_bytes = fp[file_idx]

#             if done_bytes >= size:
#                 raw = os.path.join(save_dir, tf.file_path(file_idx))
#                 final = raw + ".pdf"
#                 if os.path.exists(raw) and not os.path.exists(final):
#                     os.rename(raw, final)
#                 if os.path.exists(final):
#                     log(worker_id, f"[{tag}] Downloaded -> {os.path.basename(final)}")
#                     _queue_for_extraction(worker_id, tag, final, md5, table)
#                 else:
#                     log(worker_id, f"[{tag}] File missing after completion: {md5}")
#                     _release_in_flight(md5)
#                 del pending[file_idx]
#                 continue

#             if done_bytes > last_bytes[file_idx]:
#                 last_bytes[file_idx] = done_bytes
#                 last_progress_at[file_idx] = now
#             elif now - last_progress_at[file_idx] > STALL_TIMEOUT_S:
#                 log(worker_id, f"[{tag}] Stalled - releasing {md5}")
#                 _release_in_flight(md5)
#                 del pending[file_idx]

#         if pending:
#             time.sleep(2)

#     for file_idx, (md5, table) in pending.items():
#         log(worker_id, f"[{tag}] Timeout - releasing {md5}")
#         _release_in_flight(md5)

#     ses.remove_torrent(h)
#     log(worker_id, f"[{tag}] Job done")


# def download_worker_loop(worker_id, ses):
#     while True:
#         try:
#             job = job_queue.get(timeout=5)
#         except Empty:
#             continue
#         try:
#             process_job(worker_id, job, ses)
#         except Exception as e:
#             log(worker_id, f"[DL] Job crashed: {e}")
#             for f in job.get("files", []):
#                 _release_in_flight(f["md5"])
#         finally:
#             job_queue.task_done()


# def periodic_progress_logger(interval=15):
#     while True:
#         with done_lock:
#             done_n = len(done_md5s)
#         with dead_lock:
#             dead_n = len(dead_letter)
#         with in_flight_lock:
#             inflight_n = len(in_flight_md5s)
#         with stats_lock:
#             rows_n = total_rows_inserted
#         rate_hr = _current_rate_per_hour()
#         log(0, f"[PROGRESS] Ingested: {done_n:,} | In-flight: {inflight_n:,} | "
#                 f"Dead-letter: {dead_n:,} | Rows: {rows_n:,} | "
#                 f"Rate: {rate_hr:.0f}/hr (~{rate_hr*24:,.0f}/day) | "
#                 f"Job queue: {job_queue.qsize()}/{JOB_QUEUE_MAXSIZE} | "
#                 f"Extract queue: {extract_queue.qsize()}/{EXTRACT_QUEUE_MAXSIZE} | "
#                 f"Disk free: {disk_free_gb(BASE_SAVE_PATH):.1f}GB | RAM free: {ram_free_gb():.1f}GB")
#         time.sleep(interval)


# # ---------------------------------------------------------------------------
# # FastAPI - just receives match jobs from the matcher and reports status.
# # ---------------------------------------------------------------------------
# class FileMatch(BaseModel):
#     file_index: int
#     md5: str
#     table: str


# class JobSubmission(BaseModel):
#     magnet_uri: str
#     files: List[FileMatch]


# app = FastAPI()


# @app.post("/jobs")
# def receive_job(job: JobSubmission):
#     accepted = []
#     skipped = 0
#     for f in job.files:
#         md5 = f.md5.upper()
#         with done_lock:
#             already_done = md5 in done_md5s
#         with in_flight_lock:
#             already_in_flight = md5 in in_flight_md5s
#             if already_done or already_in_flight:
#                 skipped += 1
#                 continue
#             in_flight_md5s.add(md5)
#         accepted.append({"file_index": f.file_index, "md5": md5, "table": f.table})

#     if accepted:
#         try:
#             job_queue.put_nowait({"magnet_uri": job.magnet_uri, "files": accepted})
#         except Full:
#             for f in accepted:
#                 _release_in_flight(f["md5"])
#             return {"error": "job queue full, retry later", "accepted": 0, "skipped": skipped}

#     return {"accepted": len(accepted), "skipped": skipped}


# @app.get("/status")
# def status():
#     with done_lock:
#         done_list = list(done_md5s)
#     with dead_lock:
#         dead_n = len(dead_letter)
#     with in_flight_lock:
#         inflight_n = len(in_flight_md5s)
#     with stats_lock:
#         rows_n = total_rows_inserted
#     return {
#         "done_count": len(done_list),
#         "done_md5s": done_list,
#         "in_flight_count": inflight_n,
#         "dead_letter_count": dead_n,
#         "rows_inserted": rows_n,
#     }


# @app.get("/health")
# def health():
#     return {"ok": True}


# def main():
#     print(f"Starting {EXTRACTION_WORKERS} extraction workers...")
#     for i in range(EXTRACTION_WORKERS):
#         threading.Thread(target=extraction_worker_loop, args=(i + 1,), daemon=True).start()

#     print("Starting shared libtorrent session...")
#     ses = build_session()
#     dht_wait_deadline = time.time() + 15
#     while time.time() < dht_wait_deadline:
#         if ses.status().dht_nodes > 0:
#             break
#         time.sleep(1)
#     print(f"DHT nodes in routing table: {ses.status().dht_nodes}")

#     print(f"Starting {DOWNLOAD_WORKERS} download workers...")
#     for i in range(DOWNLOAD_WORKERS):
#         threading.Thread(target=download_worker_loop, args=(i + 1, ses), daemon=True).start()

#     threading.Thread(target=periodic_progress_logger, daemon=True).start()

#     print(f"Listening on 0.0.0.0:{PORT} ...\n")
#     uvicorn.run(app, host="0.0.0.0", port=PORT)


# if __name__ == "__main__":
#     main()











"""
MATCHER - runs on VPS #1. Standalone file, no other local files needed
except md5.csv and your part_1.txt..part_8.txt magnet lists.

Job: scan every magnet, fetch ONLY the torrent metadata (file list) --
never the actual file data -- and figure out which files match wanted
md5s. Matches get sent to the Downloader (VPS #2) over HTTP.

Install:  pip install libtorrent pandas python-dotenv requests

.env (next to this file):
    MD5_CSV_PATH       = ./md5.csv
    DOWNLOADER_URL     = http://<vps2-ip>:8000
    MAGNETS_PER_WORKER = 8
"""
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import libtorrent as lt
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
MD5_CSV_PATH         = os.getenv("MD5_CSV_PATH", "./md5.csv")
DOWNLOADER_URL        = os.getenv("DOWNLOADER_URL", "").rstrip("/")
WORKER_FILES          = [f"part_{i}.txt" for i in range(1, 9)]
MAGNETS_PER_WORKER    = int(os.getenv("MAGNETS_PER_WORKER", "8"))
SYNC_INTERVAL_S       = 30
NO_PROGRESS_BACKOFF_S = 60
HTTP_TIMEOUT_S        = 20
METADATA_TIMEOUT_S    = 90
REANNOUNCE_EVERY_S    = 30
MAX_PDF_SIZE_MB       = 60
BASE_BACKOFF_S        = 300
MAX_BACKOFF_S         = 7200

CATEGORY_TABLE_MAP = {
    "Anthropology":         "RAG_Anthropology_pg",
    "Biography":            "RAG_Biography_pg",
    "Business":             "RAG_Business_pg",
    "Communication":        "RAG_Communication_pg",
    "Criminology":          "RAG_Criminology_pg",
    "CulturalStudies":      "RAG_CulturalStudies_pg",
    "Economics":            "RAG_Economics_pg",
    "Entrepreneurship":     "RAG_Entrepreneurship_pg",
    "Film & Theatre":       "RAG_Film & Theatre_pg",
    "Finance":              "RAG_Finance_pg",
    "Geography":            "RAG_Geography_pg",
    "Health":               "RAG_Health_pg",
    "History":              "RAG_History_pg",
    "Knowledge":            "RAG_Knowledge_pg",
    "Law":                  "RAG_Law_pg",
    "Neuroscience":         "RAG_Neuroscience_pg",
    "Personal Development": "RAG_Personal Development_pg",
    "Philosophy":           "RAG_Philosophy_pg",
    "Politics":             "RAG_Politics_pg",
    "Psychology":           "RAG_Psychology_pg",
    "Religion":             "RAG_Religion_pg",
    "Science":              "RAG_Science_pg",
    "SelfHelp":              "RAG_SelfHelp_pg",
    "SocialScience":         "RAG_SocialScience_pg",
    "Sociology":             "RAG_Sociology_pg",
    "Sports":                "RAG_Sports_pg",
    "Technology":            "RAG_Technology_pg",
    "Travel":                "RAG_Travel_pg",
    "Astronomy":             "RAG_Astronomy_pg",
}

EXTRA_TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.openbittorrent.com:6969/announce",
    "udp://open.stealth.si:80/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
    "udp://tracker.tiny-vps.com:6969/announce",
    "udp://tracker.dler.org:6969/announce",
    "udp://explodie.org:6969/announce",
    "udp://tracker.moeking.me:6969/announce",
    "http://tracker.openbittorrent.com:80/announce",
]

DHT_BOOTSTRAP_NODES = (
    "router.bittorrent.com:6881,"
    "router.utorrent.com:6881,"
    "dht.transmissionbt.com:6881,"
    "dht.aelitis.com:6881,"
    "router.bitcomet.com:6881"
)

_print_lock = threading.Lock()


def log(worker_id, msg):
    with _print_lock:
        try:
            print(f"[W{worker_id:02d}] {msg}", flush=True)
        except Exception:
            pass


def build_session():
    """Self-healing: drops any settings_pack key this libtorrent build
    doesn't recognize instead of crashing on it."""
    settings = {
        "listen_interfaces":   "0.0.0.0:6881,[::]:6881",
        "enable_dht":          True,
        "enable_lsd":          True,
        "enable_natpmp":       True,
        "enable_upnp":         True,
        "dht_bootstrap_nodes": DHT_BOOTSTRAP_NODES,
        "connections_limit":   1000,
        "unchoke_slots_limit": 60,
        "active_downloads":    64,
        "active_seeds":        16,
        "active_limit":        128,
        "download_rate_limit": 0,
        "upload_rate_limit":   5 * 1024 * 1024,
        "peer_connect_timeout":    7,
        "request_queue_size":      500,
        "max_out_request_queue":   1500,
        "alert_mask":          lt.alert.category_t.all_categories,
    }
    while True:
        try:
            return lt.session(settings)
        except KeyError as e:
            msg = str(e)
            bad_key = next((k for k in settings if k in msg), None)
            if bad_key is None:
                raise
            print(f"This libtorrent build doesn't support setting '{bad_key}' - skipping it.")
            del settings[bad_key]


if not DOWNLOADER_URL:
    raise RuntimeError("DOWNLOADER_URL not set (e.g. http://<vps2-ip>:8000). Add it to your .env.")

if not os.path.exists(MD5_CSV_PATH):
    raise RuntimeError(f"{MD5_CSV_PATH} not found - put your md5.csv next to this script.")

print("Loading wanted md5 list from CSV...")
md5_df = pd.read_csv(MD5_CSV_PATH, dtype=str)
md5_df["md5"] = md5_df["md5"].str.strip().str.upper()
md5_df["category"] = md5_df["category"].str.strip()
md5_category   = dict(zip(md5_df["md5"], md5_df["category"]))
wanted_md5_set = set(md5_category.keys())
print(f"Loaded {len(wanted_md5_set):,} wanted md5s.\n")

unknown_categories = sorted(set(md5_category.values()) - set(CATEGORY_TABLE_MAP.keys()))
if unknown_categories:
    print(f"WARNING: these categories have no matching table and will never be matched: {unknown_categories}\n")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
resolved_md5s = set()   # confirmed ingested by the downloader (synced periodically)
resolved_lock = threading.Lock()

skipped_md5s  = set()   # locally known: unmapped category / oversized -> never matchable
skipped_lock  = threading.Lock()

exhausted_magnets = set()   # nothing left to ever match in this magnet
exhausted_lock    = threading.Lock()

magnet_backoff     = {}     # magnet_uri -> epoch time next eligible for retry
magnet_fail_streak = {}
backoff_lock       = threading.Lock()


def _magnet_on_cooldown(magnet_uri):
    with backoff_lock:
        return time.time() < magnet_backoff.get(magnet_uri, 0)


def _record_magnet_failure(magnet_uri):
    with backoff_lock:
        streak = magnet_fail_streak.get(magnet_uri, 0) + 1
        magnet_fail_streak[magnet_uri] = streak
        backoff = min(BASE_BACKOFF_S * (2 ** (streak - 1)), MAX_BACKOFF_S)
        magnet_backoff[magnet_uri] = time.time() + backoff
    return streak, backoff


def _record_magnet_success(magnet_uri):
    with backoff_lock:
        magnet_fail_streak.pop(magnet_uri, None)
        magnet_backoff.pop(magnet_uri, None)


def sync_with_downloader():
    try:
        resp = requests.get(f"{DOWNLOADER_URL}/status", timeout=HTTP_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
        with resolved_lock:
            before = len(resolved_md5s)
            resolved_md5s.update(md5.upper() for md5 in data.get("done_md5s", []))
            gained = len(resolved_md5s) - before
        return data, gained
    except Exception as e:
        log(0, f"[SYNC] Failed to reach downloader: {e}")
        return None, 0


def periodic_sync(stop_event):
    while not stop_event.is_set():
        data, gained = sync_with_downloader()
        if data is not None:
            log(0, f"[SYNC] Downloader: {data.get('done_count', 0):,} ingested, "
                    f"{data.get('in_flight_count', 0):,} in-flight, "
                    f"{data.get('dead_letter_count', 0):,} dead-letter, "
                    f"{data.get('rows_inserted', 0):,} rows | +{gained:,} newly resolved")
        stop_event.wait(SYNC_INTERVAL_S)


def match_worker(worker_id, magnet_uri, torrent_idx, ses):
    tag = f"MATCH-{worker_id}.{torrent_idx}"

    with exhausted_lock:
        if magnet_uri in exhausted_magnets:
            return
    if _magnet_on_cooldown(magnet_uri):
        return

    p = lt.parse_magnet_uri(magnet_uri)
    p.save_path = "/tmp/matcher_scratch"   # never actually written to
    os.makedirs(p.save_path, exist_ok=True)
    h = ses.add_torrent(p)
    h.pause()   # metadata-only: never request/download actual piece data

    for url in EXTRA_TRACKERS:
        try:
            h.add_tracker({"url": url, "tier": 1})
        except Exception:
            pass

    last_nudge = 0.0
    for i in range(METADATA_TIMEOUT_S):
        if h.status().has_metadata:
            break
        now = time.time()
        if now - last_nudge > REANNOUNCE_EVERY_S:
            try:
                h.force_dht_announce()
                h.force_reannounce()
            except Exception:
                pass
            last_nudge = now
        time.sleep(1)
    else:
        streak, backoff = _record_magnet_failure(magnet_uri)
        log(worker_id, f"[{tag}] Metadata timeout (peers={h.status().num_peers}) - "
                        f"cooling down {backoff:.0f}s (fail streak {streak})")
        ses.remove_torrent(h)
        return

    tf = h.torrent_file().files()
    nf = tf.num_files()
    md5s = [tf.file_path(i).split("/")[1].upper() for i in range(nf)]

    with resolved_lock:
        resolved_snapshot = set(resolved_md5s)

    matches = []
    for i, md5 in enumerate(md5s):
        if md5 not in wanted_md5_set:
            continue
        if md5 in resolved_snapshot:
            continue
        with skipped_lock:
            if md5 in skipped_md5s:
                continue
        category = md5_category.get(md5)
        table = CATEGORY_TABLE_MAP.get(category)
        if table is None:
            with skipped_lock:
                skipped_md5s.add(md5)
            continue
        if tf.file_size(i) > MAX_PDF_SIZE_MB * 1024 * 1024:
            with skipped_lock:
                skipped_md5s.add(md5)
            continue
        matches.append({"file_index": i, "md5": md5, "table": table})

    ses.remove_torrent(h)

    if not matches:
        log(worker_id, f"[{tag}] No usable matches - marking exhausted")
        with exhausted_lock:
            exhausted_magnets.add(magnet_uri)
        return

    try:
        resp = requests.post(
            f"{DOWNLOADER_URL}/jobs",
            json={"magnet_uri": magnet_uri, "files": matches},
            timeout=HTTP_TIMEOUT_S,
        )
        resp.raise_for_status()
        result = resp.json()
        log(worker_id, f"[{tag}] {len(matches)} match(es) -> downloader "
                        f"(accepted={result.get('accepted', 0)}, "
                        f"already_in_progress={result.get('skipped', 0)})")
        _record_magnet_success(magnet_uri)
    except Exception as e:
        # Infra/network failure talking to the downloader -- not a magnet
        # quality problem, so no backoff; it'll just be retried next pass.
        log(worker_id, f"[{tag}] Failed to notify downloader: {e}")


def run_worker_file(worker_id, txt_file, ses):
    if not os.path.exists(txt_file):
        log(worker_id, f"File not found: {txt_file} - skipping")
        return
    with open(txt_file) as f:
        magnets = [l.strip() for l in f if l.strip().startswith("magnet:")]
    if not magnets:
        return

    with ThreadPoolExecutor(max_workers=MAGNETS_PER_WORKER) as ex:
        futs = {
            ex.submit(match_worker, worker_id, uri, idx, ses): (idx, uri)
            for idx, uri in enumerate(magnets)
        }
        for fut in as_completed(futs):
            idx, uri = futs[fut]
            try:
                fut.result()
            except Exception as e:
                log(worker_id, f"ERROR magnet {idx}: {e}")


def main():
    stop_event = threading.Event()
    threading.Thread(target=periodic_sync, args=(stop_event,), daemon=True).start()

    print("Starting libtorrent session (metadata-only, no real downloads on this VPS)...")
    ses = build_session()
    dht_wait_deadline = time.time() + 15
    while time.time() < dht_wait_deadline:
        if ses.status().dht_nodes > 0:
            break
        time.sleep(1)
    print(f"DHT nodes in routing table: {ses.status().dht_nodes}\n")

    sync_with_downloader()   # one sync before the first pass

    pass_num = 0
    while True:
        pass_num += 1
        with resolved_lock:
            resolved_n = len(resolved_md5s)
        with skipped_lock:
            skipped_n = len(skipped_md5s)
        remaining_before = len(wanted_md5_set) - resolved_n - skipped_n

        if remaining_before <= 0:
            print(f"\nAll {len(wanted_md5_set):,} books accounted for "
                  f"({resolved_n:,} ingested, {skipped_n:,} un-matchable). Stopping.\n")
            break

        print(f"\n{'-'*60}\nPass {pass_num}: {remaining_before:,} book(s) still need matching\n{'-'*60}\n")

        with ThreadPoolExecutor(max_workers=len(WORKER_FILES)) as ex:
            futs = {
                ex.submit(run_worker_file, i + 1, txt, ses): txt
                for i, txt in enumerate(WORKER_FILES)
            }
            for fut in as_completed(futs):
                txt = futs[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f"[ERROR] Worker file {txt}: {e}", flush=True)

        sync_with_downloader()
        with resolved_lock:
            resolved_n = len(resolved_md5s)
        with skipped_lock:
            skipped_n = len(skipped_md5s)
        remaining_after = len(wanted_md5_set) - resolved_n - skipped_n
        print(f"Pass {pass_num} complete - {remaining_after:,} book(s) still remaining.\n")

        if remaining_after >= remaining_before and remaining_after > 0:
            log(0, f"No progress in pass {pass_num} - backing off {NO_PROGRESS_BACKOFF_S}s before next pass.")
            time.sleep(NO_PROGRESS_BACKOFF_S)

    stop_event.set()


if __name__ == "__main__":
    main()