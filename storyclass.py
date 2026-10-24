#!/usr/bin/env python3
"""storyclass — classify stories with an Ollama decision model (clef-flash).

Reads a JSON-schema file (object of boolean properties with descriptions)
or a questions file (field -> {type, instructions}) and sends each question
as a `noul` question to Ollama's /v1/systemone endpoint in a single joint
request.

Usage:
  ./storyclass.py schema.json story.txt            # print JSON to stdout
  ./storyclass.py schema.json story1.txt story2.txt
  ./storyclass.py schema.json -o out.jsonl *.txt   # one JSON object per line (appends across runs)
  ./storyclass.py schema.json dir/                 # classify every file in dir (recursive)
  find -ctime -10 -type f | ./storyclass.py --stdin schema.json   # filenames from stdin
  # re-running with the same -o file skips stories already present in it (resume)
  ./storyclass.py schema.json 'a/b/a*'             # glob patterns work (quote to pass through)
  ./storyclass.py schema.json                      # no stories: print the converted questions
  # editable questions file: generate what the decider actually receives, edit the
  # instructions by hand, then pass that file in place of the schema:
  ./storyclass.py --gen-questions questions.json schema.json
  $EDITOR questions.json
  ./storyclass.py questions.json dir/ -o results.jsonl
  # NOTE: resume skipping matches on the exact filename string in the output
  # file, so invoke the same way each time (same relative/absolute paths).
  ./storyclass.py schema.json story.txt -s 30000   # send up to 30 KiB of story
  ./storyclass.py schema.json story.txt --min 0.7  # only report fields >= 0.7 as true
  ./storyclass.py schema.json story.txt --verbose  # include all probabilities
  ./storyclass.py --host 10.0.0.223 --model clef schema.json story.txt
  ./storyclass.py schema.json bigdir -o results.jsonl --bg   # detached: survives Ctrl-C / logout
  ./storyclass.py --list-models                    # show the 5 decision models
  # distributed: each --host (or --hosts file line) is a parallel worker;
  # repeat a host to run more workers on it (no dedup). Trailing tokens per
  # worker, in order: model, batch (questions/request), size (max chars),
  # ctx (context window in tokens — always sent to the server as num_ctx):
  ./storyclass.py --host mygateway.com:1234 clef 12 \
                  --host 183.6.99.190:5003 clef 1 7000 16384 \
                  --host otherbox:11434 nimble schema.json dir/ -o results.jsonl
  cat hosts.txt   # one 'host:port [model] [batch] [size] [ctx]' per line
  ./storyclass.py --hosts hosts.txt schema.json dir/ -o results.jsonl
"""

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.request
from html import unescape as html_unescape
from pathlib import Path

# Decision models available on Ollama (requires Ollama 0.35.1+).
DECISION_MODELS = ["clef-flash", "clef", "nimble", "jev", "systemone"]

DEFAULT_HOST = "10.0.0.223"
DEFAULT_MODEL = "clef-flash"


def parse_host_spec(spec: str, port: int = None):
    """Parse a 'host:port [model] [batch_size]' spec into (url, model, batch_size).

    Accepts:
      host                          -> http://host:11434
      host:port                     -> http://host:port
      host:port/path ...            -> http://host:port/path...
      scheme://host                 -> scheme://host:80/443
      scheme://host:port            -> scheme://host:port
      scheme://host:port/path ...   -> scheme://host:port/path...
      scheme://host/path ...        -> scheme://host:default-port/path...
    Trailing whitespace-separated tokens (in order):
      model       (any non-numeric token, e.g. 'clef')
      batch_size  (a bare integer, e.g. '12' — questions per request)
      slice_size  (a second bare integer, e.g. '7000' — max story chars sent)
      num_ctx     (a third bare integer, e.g. '16384' — context window in tokens)
    Examples:
      10.0.0.223 clef 12            -> clef, 12 questions/request
      183.6.99.190:5003 clef 1 7000 16384 -> clef, 1 q/req, 7000-char slices, 16k ctx
    """
    parts = spec.split()
    hostval = parts[0]
    model = None
    batch = None
    size = None
    ctx = None
    for tok in parts[1:]:
        if tok.isdigit():
            if batch is None:
                batch = int(tok)
            elif size is None:
                size = int(tok)
            elif ctx is None:
                ctx = int(tok)
        elif model is None:
            model = tok
        # any further non-numeric token is ignored (defensive)
    extra_path = ""
    scheme = "http"
    pport = port  # --port override; applied only when no explicit port given
    if hostval.startswith("https://"):
        scheme, hostval = "https", hostval[len("https://"):]
    elif hostval.startswith("http://"):
        scheme, hostval = "http", hostval[len("http://"):]
    # split off any trailing path
    if "/" in hostval:
        hostport, extra_path = hostval.split("/", 1)
    else:
        hostport = hostval
    # split off an explicit numeric port
    if ":" in hostport:
        h, p = hostport.rsplit(":", 1)
        if p.isdigit():
            hostval, pport = h, int(p)
        else:
            hostval = hostport  # not a numeric port; treat whole thing as host
    else:
        hostval = hostport
    if pport is None:
        pport = 443 if scheme == "https" else 11434
    url = f"{scheme}://{hostval}:{pport}/{extra_path.strip('/')}".rstrip("/")
    return url, model, batch, size, ctx

def schema_to_questions(data, path):
    """Convert a JSON schema (with boolean properties) to the questions dict.

    Accepts both the wrapped {"schema": {...}} form and a bare JSON schema.
    """
    if "schema" in data and isinstance(data["schema"], dict):
        data = data["schema"]
    props = data.get("properties")
    if not props:
        raise ValueError(f"no 'properties' found in {path} (a questions file instead maps "
                         f"field -> {{'type','instructions'}} — see --gen-questions)")
    questions = {}
    for name, spec in props.items():
        desc = (spec.get("description") or name)
        # Strip a leading "true if ..." prefix — it's redundant in a question.
        for prefix in ("true if ", "True if "):
            if desc.startswith(prefix):
                desc = desc[len(prefix):]
                break
        questions[name] = {"type": "noul", "instructions": desc}
    return questions


def load_questions(path: Path):
    """Load the questions to send — either a schema (converted) or a
    questions file (used directly, as generated by --gen-questions).

    A questions file maps field name -> {"type", "instructions"} (optionally
    wrapped in {"questions": {...}}). Every value must carry an
    "instructions" string; that's what distinguishes it from a schema.
    """
    data = json.loads(path.read_text())
    if isinstance(data, dict) and isinstance(data.get("questions"), dict):
        data = data["questions"]
    if (isinstance(data, dict) and data
            and all(isinstance(v, dict) and v.get("instructions") for v in data.values())):
        questions = {}
        for name, spec in data.items():
            questions[name] = {"type": spec.get("type", "noul"), "instructions": spec["instructions"]}
        return questions
    return schema_to_questions(data, path)


# The /v1/systemone endpoint rejects requests whose prompt exceeds the model's
# context (and the payload is capped at 64 KiB). We control the prompt size by
# truncating the story to a target character budget. Tokens are hard to count,
# so we budget at ~chars/3.5 tokens (buffer under the usual 4/char) with room
# left for the question scaffolding. -s overrides the per-model default.
MODEL_CHAR_LIMITS = {
    "tev1": 3500,        # 2050-token hard limit; ~10-q batch scaffold ~930 tokens leaves ~1.1k for story
    "clef-flash": 15000, # 8194-token prompt limit; ~4.6k question scaffolding + 15k/3.5
    "clef": 20000,
    "nimble": 12000,     # 8194-token prompt limit, same math as clef-flash
    "jev": 20000,
    "systemone": 20000,
}
DEFAULT_CHARS = 20000

# Models with tiny prompt windows can't fit all 48 questions in one request
# (48-q scaffold alone is ~3.8k tokens, over tev1's 2050 limit). Split the
# questions into N batches of requests per story and merge the answers.
# Questions per request for small-window models. The 48-question scaffold
# alone is ~3.8k tokens (over tev1's 2050 limit), so its questions must be
# split. --batch_size overrides; 1 = all questions in one request.
MODEL_BATCHES = {
    "tev1": 12,          # ~12 questions/request (scaffold ~1.1k tok + story fits 2050)
}
# Models without an entry get no question splitting (all questions, one request).
DEFAULT_BATCHES = None

# Every request declares the context window it needs (num_ctx), always. Without
# it the server falls back to its own default (often 4096) and rejects anything
# larger with "input too large" — which is what the auto-split ladder exists to
# paper over. Precedence: per-host spec token > --num-ctx > per-model default
# > DEFAULT_NUM_CTX. Set these to the server's real limits (or override per run
# with --num-ctx / the host spec's 4th token).
MODEL_NUM_CTX = {
    "tev1": 2048,        # tiny-window model
    "clef-flash": 8192,
    "nimble": 8192,
    "clef": 16384,
    "jev": 16384,
    "systemone": 16384,
}
DEFAULT_NUM_CTX = 8192


# --- HTML story files -------------------------------------------------------
# Some corpus files are HTML (scans/archives). For those, do the minimal work
# to get the text: drop script/style blocks, turn block-end tags and <br> into
# newlines, strip the remaining tags, unescape entities. No external parser.
_HTML_EXTS = {".html", ".htm", ".xhtml"}
_RE_SCRIPT_STYLE = re.compile(r"<(script|style)[^>]*>.*?</\1\s*>", re.IGNORECASE | re.DOTALL)
_RE_BLOCK_END = re.compile(r"</\s*(p|div|h[1-6]|li|blockquote|tr|pre)\s*>", re.IGNORECASE)
_RE_BR = re.compile(r"<\s*br\s*/?\s*>[ \t]*\n?", re.IGNORECASE)
_RE_TAG = re.compile(r"<[^>]+>")


def strip_html(text: str) -> str:
    text = _RE_SCRIPT_STYLE.sub(" ", text)
    text = _RE_BLOCK_END.sub("\n\n", text)     # paragraph breaks (cap handles doubles)
    text = _RE_BR.sub("\n", text)
    text = _RE_TAG.sub(" ", text)
    text = html_unescape(text).replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)          # tag removal leaves space runs
    text = re.sub(r" ?\n ?", "\n", text)         # no leading/trailing space on lines
    text = re.sub(r"\n{3,}", "\n\n", text)       # cap blank lines at one
    return text.strip()


def is_html_file(f, raw: str) -> bool:
    if Path(f).suffix.lower() in _HTML_EXTS:
        return True
    head = raw[:80].lstrip().lower()
    return head.startswith("<!doctype html") or head.startswith("<html")


def read_story(f) -> str:
    """Read a story file as text. HTML files (by extension or doctype) are
    stripped to plain text first, so the model sees story, not markup."""
    raw = Path(f).read_text(errors="replace").strip()
    if is_html_file(f, raw):
        stripped = strip_html(raw)
        if stripped:
            print(f"info: {f}: stripped HTML ({len(raw)} -> {len(stripped)} chars)", file=sys.stderr)
        return stripped
    return raw


def truncate_story(text: str, max_chars: int) -> tuple[str, bool]:
    """Cut story text to max_chars. Returns (story, was_truncated)."""
    if len(text) <= max_chars:
        return text, False
    return text[:max_chars] + f"\n[... story truncated to {max_chars} chars ...]", True


def classify(url: str, model: str, story: str, questions: dict, keep_alive: str = "15m", cookie: str = None, num_ctx: int = None):
    payload_obj = {
        "model": model,
        "state": story,
        "questions": questions,
        "keep_alive": keep_alive,
    }
    if num_ctx is not None:
        # Always sent by classify_file (resolved per worker); None only for
        # direct/legacy callers. Top-level to match how this endpoint takes
        # keep_alive; flip to payload_obj["options"]["num_ctx"] if the server
        # wants the nested form.
        payload_obj["num_ctx"] = num_ctx
    payload = json.dumps(payload_obj).encode()
    endpoint = f"{url}/v1/systemone"
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")[:200]
        if e.code == 404:
            raise SystemExit(
                f"error: {endpoint} returned 404. The decision endpoint requires "
                f"Ollama 0.35.1+ (check the host). Server body: {body!r}\n"
                f"Tip: run `ollama --version` on the host at {url}"
            ) from None
        raise SystemExit(f"error: {endpoint} -> HTTP {e.code}: {body}") from None


def collect_files(paths):
    """Each arg may be a literal path, a glob pattern (a/b/a*), or a directory.

    Globs are expanded by fnmatch/glob so unquoted shell patterns work too.
    Directories are walked recursively. Order is preserved, duplicates removed.
    """
    import glob as _glob
    files = []
    seen = set()
    for p in paths:
        # Expand a glob pattern if it matches anything.
        matches = _glob.glob(p) if any(c in p for c in "*?[" ) else []
        if matches:
            candidates = matches
        else:
            candidates = [p]
        for c in candidates:
            path = Path(c)
            if path.is_dir():
                found = sorted(f for f in path.rglob("*") if f.is_file())
            else:
                found = [path]
            for f in found:
                key = str(f)
                if key not in seen:
                    seen.add(key)
                    files.append(f)
    return files


def main():
    # Line-buffer stdout so per-story labels show up live when piped/redirected
    # (stderr is already unbuffered). Without this, piped stdout is block-
    # buffered and the incremental [i/N] lines only flush at exit.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("schema", nargs="?",
                    help="schema JSON (boolean properties) OR a questions JSON file "
                         "(field -> {type, instructions}, as written by --gen-questions)")
    ap.add_argument("stories", nargs="*", help="story file(s), or a directory of .txt files")
    ap.add_argument("--host", action="append", default=[], metavar="HOST[:PORT][ MODEL][ BATCH][ SIZE][ CTX]",
                    help=f"Ollama worker host, e.g. 10.0.0.223 (default: a single {DEFAULT_HOST}). "
                         "Each occurrence adds a worker — repeat it to run parallel workers. "
                         "Trailing tokens (in order): model, batch (questions/request), "
                         "slice size (max story chars sent), ctx (context window tokens): "
                         "--host mygateway.com:1234 clef 12 --host 183.6.99.190:5003 clef 1 7000 16384")
    ap.add_argument("--hosts", default=None,
                    help="file with one 'host:port [model] [batch] [size] [ctx]' spec per line; every line is a "
                         "worker (repeated lines are repeated workers, no dedup) — "
                         "e.g. '183.6.99.190:5003 clef 1 7000 16384' four times = 4 workers, 1 q/req, "
                         "7000-char slices, 16k context")
    ap.add_argument("--port", type=int, default=None,
                    help="Ollama port for --host specs without an explicit port "
                         "(default 11434 for plain hosts; 80/443 for http(s):// URLs)")
    ap.add_argument("--model", default=DEFAULT_MODEL,
                    help=f"decision model for workers without their own, one of {', '.join(DECISION_MODELS)} "
                         f"(default {DEFAULT_MODEL})")
    ap.add_argument("--list-models", action="store_true", help="list the known decision models and exit")
    ap.add_argument("-s", "--size", type=int, default=None,
                    help="max story characters to send (default per model: "
                         + ", ".join(f"{k}={v}" for k, v in MODEL_CHAR_LIMITS.items())
                         + "). Explicit value overrides the model default")
    ap.add_argument("--start", type=int, default=200,
                    help="skip the first N characters of each file before classifying "
                         "(default 200; headers/metadata are structural, not story; "
                         "for HTML files the count is after tag stripping)")
    ap.add_argument("--maxsize", type=int, default=None,
                    help="split the story into sequential chunks of N chars, classify each, "
                         "and average the probabilities (default None = one request, whole story)")
    ap.add_argument("--min", type=float, default=0.5, help="probability threshold for a field to be true (default 0.5)")
    ap.add_argument("--batch_size", type=int, default=None,
                    help="split questions into groups of this many per request (the decision "
                         "batch size — how many questions the model processes in one prompt; "
                         "1 = one question per request) "
                         "(default per model: " + ", ".join(f"{k}={v}" for k, v in MODEL_BATCHES.items())
                         + "; all questions in one request otherwise)")
    ap.add_argument("--json", action="store_true", help="print the true fields as a JSON array instead of one per line")
    ap.add_argument("--verbose", action="store_true", help="include every field with its probability")
    ap.add_argument("--num-ctx", type=int, default=None,
                    help="context window (tokens) — ALWAYS sent to the server as num_ctx. "
                         "Precedence: per-host spec token > this flag > per-model default (" + ", ".join(
                             f"{k}={v}" for k, v in MODEL_NUM_CTX.items()) + f") > {DEFAULT_NUM_CTX}. "
                         "Set it to the server's real limit so large prompts stop being rejected "
                         "with 'input too large'")
    ap.add_argument("--cookie", default=None,
                    help="cookie header value to send with each request "
                         "(e.g. 'session=abc123'); also read from OLLAMA_COOKIE env var")
    ap.add_argument("--stdin", action="store_true",
                    help="read story filenames from stdin (one per line), "
                         "e.g. 'find -ctime -10 -type f | ./storyclass.py --stdin schema.json'")
    ap.add_argument("--bg", action="store_true",
                    help="detach into the background (survives Ctrl-C and terminal exit); "
                         "output goes to --log; prints the PID and exits")
    ap.add_argument("--retries", type=int, default=3, help="attempts per story before giving up (default 3)")
    ap.add_argument("--log", default="storyclass.log", help="log file for --bg runs (default storyclass.log)")
    ap.add_argument("--gen-questions", nargs="?", const="", default=None, metavar="FILE",
                    help="write the exact questions the decider will receive (field -> "
                         "{type, instructions}) to FILE (default <schema>.questions.json) and "
                         "exit; edit the instructions, then pass that file in place of the "
                         "schema: ./storyclass.py questions.json dir/ -o results.jsonl")
    ap.add_argument("-o", "--out", help="write JSON lines (one object per story) to this file")
    args = ap.parse_args()

    if args.bg:
        # Detach and spawn ourselves in a new session: survives Ctrl-C and
        # terminal close. Output is appended to the log file.
        import subprocess
        fwd = [a for a in sys.argv[1:] if a != "--bg"]
        with open(args.log, "a") as lf:
            lf.write(f"=== storyclass run started {time.ctime()} (parent pid {os.getpid()}) ===\n")
            lf.flush()
            proc = subprocess.Popen(
                [sys.executable, os.path.abspath(__file__)] + fwd,
                stdout=lf,
                stderr=subprocess.STDOUT,
                # inherit stdin so piped filename lists (--stdin) reach the child
                start_new_session=True,
                cwd=os.getcwd(),
            )
        print(f"running detached: pid {proc.pid}")
        print(f"log:  {os.path.abspath(args.log)}")
        print(f"tail: tail -f {args.log}")
        print(f"stop: kill {proc.pid}")
        return

    if args.list_models:
        print("decision models (Ollama 0.35.1+ required):")
        for m in DECISION_MODELS:
            note = "  (default)" if m == DEFAULT_MODEL else ""
            print(f"  {m}{note}")
        return

    if not args.schema:
        ap.error("a schema (or questions) JSON file is required")

    cookie = args.cookie or os.environ.get("OLLAMA_COOKIE")

    # --gen-questions: emit the exact question text the decider receives, so it
    # can be hand-edited and used directly on later runs (bypassing the schema).
    if args.gen_questions is not None:
        src = Path(args.schema)
        questions = load_questions(src)
        out = (Path(args.gen_questions) if args.gen_questions
               else src.with_name(src.stem + ".questions.json"))
        out.write_text(json.dumps(questions, indent=2) + "\n")
        print(f"wrote {len(questions)} questions to {out}\n"
              f"edit the 'instructions' freely, then use it directly:\n"
              f"  ./storyclass.py {out} <stories> -o results.jsonl", file=sys.stderr)
        return

    # Build the worker list: repeated --host args and/or --hosts file lines.
    # NO dedup — repeating a host (or a line in the file) is how you add more
    # parallel workers on the same machine; each occurrence is one worker.
    specs = list(args.host)
    if args.hosts:
        with open(args.hosts) as fh:
            for line in fh:
                line = line.strip()
                if line and not line.startswith("#"):
                    specs.append(line)
    if not specs:
        specs = [DEFAULT_HOST]
    workers = []
    for spec in specs:
        url, wmodel, wbatch, wsize, wctx = parse_host_spec(spec, port=args.port)
        workers.append({"url": url, "model": wmodel or args.model, "batch": wbatch,
                        "size": wsize, "ctx": wctx})
    if len(workers) > 1:
        print(f"workers ({len(workers)}): " + ", ".join(
            f"{w['url']} [{w['model']}]"
            + (f" bs={w['batch']}" if w["batch"] else "")
            + (f" s={w['size']}" if w["size"] else "")
            + (f" ctx={w['ctx']}" if w["ctx"] else "")
            for w in workers), file=sys.stderr)

    # Per-model defaults (size, batch) come from the first worker's model
    # (or --model). If workers mix models and no --batch_size was given, we
    # leave batch_size None (one request per story) and let the per-host
    # auto-shrink handle oversized prompts.
    eff_model = workers[0]["model"]
    models_used = {w["model"] for w in workers}
    size_flag = args.size   # non-None only if the user passed -s/--size
    if not args.size:
        args.size = MODEL_CHAR_LIMITS.get(eff_model, DEFAULT_CHARS)

    files = collect_files(args.stories)
    if args.stdin:
        stdin_files = [l.strip() for l in sys.stdin if l.strip()]
        if not stdin_files:
            ap.error("no filenames received on stdin")
        files = collect_files(stdin_files)
    questions = load_questions(Path(args.schema))
    if len(questions) > 64:
        print(f"warning: {len(questions)} questions exceeds the 64-question limit", file=sys.stderr)

    # Context floor: the num_ctx we request must fit the largest prompt we
    # plan to send — the first slice (size chars ~= size/3 tokens; 3
    # chars/token is conservative) plus the question scaffold plus output
    # headroom. An explicit --size 100000 must not ship with the 16384 model
    # default or every slice 500s again. Applied only when the size is
    # explicit (-s or host token) and no ctx was pinned: an explicit
    # --num-ctx / host ctx token encodes a known server limit and wins,
    # with the auto-split ladder covering any slice that still overflows.
    for w in workers:
        ctx_explicit = w.get("ctx") or args.num_ctx is not None
        size_explicit = bool(w.get("size")) or bool(size_flag)
        ctx_final = w.get("ctx") or args.num_ctx or MODEL_NUM_CTX.get(w["model"], DEFAULT_NUM_CTX)
        if size_explicit and not ctx_explicit:
            size = w.get("size") or args.size
            min_ctx = size // 3 + len(json.dumps(questions)) // 3 + 2048
            if min_ctx > ctx_final:
                print(f"info: {w['url']}: raising num_ctx {ctx_final} -> {min_ctx} "
                      f"so {size}-char slices + questions fit the window", file=sys.stderr)
                ctx_final = min_ctx
        w["ctx_final"] = ctx_final

    # No stories given: print the questions for inspection and exit.
    if not files:
        print(json.dumps(questions, indent=2))
        print(f"# {len(questions)} questions total (pass a questions file to edit them: --gen-questions)",
              file=sys.stderr)
        return

    # Question batching (for small-window models). Each worker uses its own
    # model's batch size when calling classify_file; this global q_chunks is
    # only used for the "splitting N questions into M batches" log line.
    qkeys = list(questions)
    n_q = len(qkeys)

    def make_q_chunks(b, qs=None, qk=None):
        # b = questions per request. b=1 -> one question per request.
        # b=None or b>=n_q -> everything in one request.
        qs = qs if qs is not None else questions
        qk = qk if qk is not None else qkeys
        if b and 1 <= b < len(qk):
            return [{k: qs[k] for k in qk[i:i + b]} for i in range(0, len(qk), b)]
        return [qs]

    bs = args.batch_size
    if len(models_used) == 1:
        # all workers share one model: use that model's default if not given
        if not bs:
            bs = MODEL_BATCHES.get(eff_model, DEFAULT_BATCHES)
    q_chunks = make_q_chunks(bs)
    if len(q_chunks) > 1:
        models_str = ", ".join(sorted(models_used))
        print(f"model(s) {models_str}: splitting {n_q} questions into {len(q_chunks)} batches "
              f"of {bs} per story", file=sys.stderr)

    # Resume support: skip files already present in the output file, so a
    # re-run only processes the remainder.
    if args.out and Path(args.out).exists():
        existing = set()
        with open(args.out) as eh:
            for line in eh:
                line = line.strip()
                if not line:
                    continue
                try:
                    existing.add(json.loads(line).get("filename"))
                except json.JSONDecodeError:
                    pass
        before = len(files)
        files = [f for f in files if str(f) not in existing]
        skipped = before - len(files)
        if skipped:
            print(f"skipping {skipped} already in {args.out}; {len(files)} left to process", file=sys.stderr)
        if not files:
            print("all stories already processed, nothing to do", file=sys.stderr)
            return

    # Open the output file in APPEND mode so a re-run adds to it instead of
    # clobbering it, and write + flush each record as it completes — a crash
    # or Ctrl-C mid-run keeps everything done so far. A lock guards concurrent
    # worker writes.
    out_fh = open(args.out, "a") if args.out else None
    write_lock = threading.Lock()
    done = 0
    failed = []
    total = len(files)
    t_start = time.monotonic()

    # Distribute files across workers: each worker takes every Nth file
    # (round-robin), so they run in parallel. With a single worker this is
    # just sequential processing.
    if len(workers) > 1:
        import concurrent.futures
        files_per_worker = {i: [] for i in range(len(workers))}
        for idx, f in enumerate(files):
            files_per_worker[idx % len(workers)].append(f)

        def run_worker(wi, wf):
            nonlocal done
            rec = []
            for f in wf:
                raw = read_story(f)
                if args.start:
                    raw = raw[args.start:].lstrip()
                if not raw:
                    print(f"warning: {f} is empty, skipping", file=sys.stderr)
                    continue
                res = classify_file(f, raw, [workers[wi]], questions, args, out_fh, cookie, write_lock)
                if res is not None:
                    with write_lock:
                        done += 1
                        # report immediately as each story completes
                        answers, t_file, label = res[1], res[2], res[3]
                        try:
                            print(f"[{done}/{total}] ({t_file:.1f}s) {f} :: {label}")
                        except BrokenPipeError:
                            devnull = os.open(os.devnull, os.O_WRONLY)
                            os.dup2(devnull, sys.stdout.fileno())
                            sys.exit(0)
                        if done % 10 == 0 and done < total:
                            elapsed = time.monotonic() - t_start
                            rate = done / elapsed if elapsed > 0 else 0
                            remaining = total - done
                            if rate > 0:
                                eta = remaining / rate
                                if eta < 60:
                                    eta_s = f"{eta:.0f}s"
                                elif eta < 3600:
                                    eta_s = f"{eta / 60:.0f} min"
                                else:
                                    eta_s = f"{eta / 3600:.1f} h"
                                print(f"  ... {done}/{total} done | est remaining: {remaining} stories ~ {eta_s} "
                                      f"at {rate:.2f} stories/s", file=sys.stderr)
                    rec.append((f, res))
                else:
                    with write_lock:
                        failed.append(str(f))
            return rec

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(workers)) as ex:
            futs = [ex.submit(run_worker, i, files_per_worker[i]) for i in range(len(workers))]
            for fut in concurrent.futures.as_completed(futs):
                fut.result()   # propagate worker exceptions
    else:
        for i, f in enumerate(files, 1):
            raw = read_story(f)
            if args.start:
                raw = raw[args.start:].lstrip()
            if not raw:
                print(f"warning: {f} is empty, skipping", file=sys.stderr)
                continue
            res = classify_file(f, raw, workers, questions, args, out_fh, cookie, write_lock)
            if res is None:
                failed.append(str(f))
                continue
            with write_lock:
                done += 1
            answers, t_file, label = res[1], res[2], res[3]
            try:
                print(f"[{i}/{total}] ({t_file:.1f}s) {f} :: {label}")
            except BrokenPipeError:
                devnull = os.open(os.devnull, os.O_WRONLY)
                os.dup2(devnull, sys.stdout.fileno())
                sys.exit(0)
            if done % 10 == 0 and done < total:
                elapsed = time.monotonic() - t_start
                rate = done / elapsed if elapsed > 0 else 0
                remaining = total - done
                if rate > 0:
                    eta = remaining / rate
                    if eta < 60:
                        eta_s = f"{eta:.0f}s"
                    elif eta < 3600:
                        eta_s = f"{eta / 60:.0f} min"
                    else:
                        eta_s = f"{eta / 3600:.1f} h"
                    print(f"  ... {done}/{total} done | est remaining: {remaining} stories ~ {eta_s} "
                          f"at {rate:.2f} stories/s", file=sys.stderr)
                else:
                    print(f"  ... {done}/{total} done | est remaining: {remaining} stories (no rate yet)", file=sys.stderr)

    if out_fh:
        out_fh.close()
    elapsed = time.monotonic() - t_start
    rate = done / elapsed if elapsed > 0 and done else 0
    summary = (f"hosts={len(workers)}w models={','.join(sorted(models_used))} done={done} failed={len(failed)} "
               f"elapsed={elapsed:.0f}s rate={rate:.2f} stories/s")
    if done:
        remaining = total - done
        if remaining > 0:
            summary += f" est_remaining={remaining / rate / 60:.0f} min" if rate else f" {remaining} left"
    print(summary, file=sys.stderr)
    print(f"wrote {done} record(s)" + (f" to {args.out}" if args.out else ""), file=sys.stderr)
    if failed:
        fail_list = "\n".join(failed)
        print(f"FAILED ({len(failed)}):\n{fail_list}", file=sys.stderr)
        if args.out:
            with open(args.out + ".failed", "w") as fh:
                fh.write(fail_list + "\n")
            print(f"failed file list written to {args.out}.failed", file=sys.stderr)
        sys.exit(1)


def classify_file(f, raw, workers, questions, args, out_fh, cookie, write_lock=None):
    """Classify one story file against the given workers.

    Uses workers[0] as the single host to hit (one request per slice/question-
    batch, sequential). Workers are for parallel file assignment at the caller
    level, not per-request load balancing. Returns (record_or_None, answers,
    elapsed, label). record is None if the file failed. Writes the record to
    out_fh (locked) if provided.
    """
    t_file = time.monotonic()
    w = workers[0]
    url, model = w["url"], w["model"]
    # Question batches: this worker's explicit batch (from the host spec,
    # 'host:port model 12') wins; else the global --batch_size; else the
    # model's default.
    b = w.get("batch") or args.batch_size
    if not b:
        b = MODEL_BATCHES.get(model, DEFAULT_BATCHES)
    # Slice size: this worker's explicit size (host spec, 4th token) wins;
    # else the global --size / model default.
    slice_default = w.get("size") or args.size
    # Context window: always sent. Resolved per worker in main() (host token
    # > --num-ctx > model default) and floored so it fits the largest slice
    # we send — see the "Context floor" block in main().
    ctx = w["ctx_final"]

    def make_q_chunks(bb, qs=questions, qk=None):
        qk = qk if qk is not None else list(qs)
        if bb and 1 <= bb < len(qk):
            return [{k: qs[k] for k in qk[i:i + bb]} for i in range(0, len(qk), bb)]
        return [qs]
    batch = make_q_chunks(b)

    # Chunking: slice the story into sequential pieces of the model's char
    # budget (args.size) — 0..15000, 15000..30000, 30000..45000 — send
    # each as its own request, and average the probabilities. Stop once
    # --maxsize is reached (default: no cap). Short stories that fit in
    # one slice still cost exactly one request.
    slice_size = slice_default
    stop = args.maxsize if args.maxsize else len(raw)
    stop = min(stop, len(raw))
    story_chunks = [raw[j:j + slice_size] for j in range(0, stop, slice_size)]
    if len(story_chunks) > 1:
        print(f"info: {f} split into {len(story_chunks)} slices of {slice_size} chars, "
              f"probabilities will be averaged", file=sys.stderr)

    # One question batch x one story chunk = one request. Every request
    # must succeed for the file to count.
    # If the server rejects a request as "too large" (e.g. HTTP 500
    # 'input (N tokens) is too large ... increase the physical batch
    # size' — that "batch size" is the SERVER's own token budget, not
    # our --batch_size), we shrink what we send: first the story slice
    # is split in half and BOTH halves are classified (recursively, up
    # to level 4, min piece slice_size//8 chars); a level-L piece
    # contributes weight 1/2**L so a fully covered slice still totals
    # 1.0. If the slice is too small to split and the prompt is still
    # too large, the question batch is halved (min 2) and the slice is
    # re-asked.
    last_err = None
    sum_w = {}   # name -> weighted probability total
    cnt_w = {}   # name -> weight total (1.0 per fully covered slice)
    input_tokens = 0
    ok = True
    req_no = 0
    MAX_LEVEL = 4
    MIN_PIECE = max(400, slice_size // 8)
    pending = [(sc, 0) for sc in story_chunks]
    while pending and ok:
        sc, level = pending.pop(0)
        sc_sum = {}
        sc_cnt = {}
        status = "ok"          # ok | split | rebatch | fail
        sc_tokens = 0
        for chunk in batch:
            req_no += 1
            result = None
            for attempt in range(1, args.retries + 1):
                try:
                    result = classify(url, model, sc, chunk, cookie=cookie, num_ctx=ctx)
                    break
                except SystemExit as e:
                    msg = str(e)
                    code = msg.split("-> HTTP ")[1].split(" ")[0] if "-> HTTP " in msg else ""
                    if "too large" in msg or "batch size" in msg or "tokens) is too" in msg:
                        if level < MAX_LEVEL and len(sc) > 2 * MIN_PIECE:
                            print(f"info: {f} req {req_no} too large (HTTP {code}); "
                                  f"splitting {len(sc)}-char slice -> 2 x {len(sc) // 2} "
                                  f"(level {level + 1})", file=sys.stderr)
                            status = "split"
                            break
                        cur_bs = b or n_q
                        if cur_bs > 2:
                            nb = max(2, cur_bs // 2)
                            print(f"info: {f} req {req_no} too large (HTTP {code}); "
                                  f"shrinking decision batch {cur_bs} -> {nb} questions/request, "
                                  f"re-asking this slice", file=sys.stderr)
                            b = nb
                            batch = make_q_chunks(nb)
                            status = "rebatch"
                            break
                        last_err = msg.splitlines()[0]
                        print(f"error: {f} req {req_no}: prompt still too large at "
                              f"{len(sc)} chars with {cur_bs} questions/request: "
                              f"{msg.splitlines()[0]}", file=sys.stderr)
                        status = "fail"
                        break
                    if code in ("429",) or code.startswith("5"):
                        last_err = msg.splitlines()[0]
                        print(f"warning: {f} req {req_no} attempt {attempt}/{args.retries}: {url} -> HTTP {code}", file=sys.stderr)
                        time.sleep(2 * attempt)
                        continue
                    if code == "404":
                        # Systemic (wrong host/version), not a file problem — abort.
                        raise
                    # Other hard error: skip this file, keep the run going.
                    print(f"error: {f} req {req_no}: {msg.splitlines()[0]}", file=sys.stderr)
                    last_err = msg.splitlines()[0]
                    status = "fail"
                    break
                except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                    last_err = f"{url}: {e}"
                    print(f"warning: {f} req {req_no} attempt {attempt}/{args.retries}: {url}: {e}", file=sys.stderr)
                    time.sleep(2 * attempt)
            if result is None:
                if status == "ok":
                    status = "fail"   # retries exhausted (429/5xx/network)
                break
            for name, a in result.get("answers", {}).items():
                sc_sum[name] = sc_sum.get(name, 0.0) + a["noul"]
                sc_cnt[name] = sc_cnt.get(name, 0) + 1
            sc_tokens += result.get("usage", {}).get("input_tokens", 0)
        if status == "ok":
            wgt = 1.0 / (2 ** level)
            for name, p in sc_sum.items():
                sum_w[name] = sum_w.get(name, 0.0) + (p / sc_cnt[name]) * wgt
                cnt_w[name] = cnt_w.get(name, 0.0) + wgt
            input_tokens += sc_tokens
        elif status == "split":
            mid = len(sc) // 2
            pending.insert(0, (sc[:mid], level + 1))
            pending.insert(1, (sc[mid:], level + 1))
        elif status == "rebatch":
            pending.insert(0, (sc, level))
        else:
            ok = False
    if not ok:
        print(f"error: {f} failed after {args.retries} attempts: {last_err}", file=sys.stderr)
        return None

    # Weighted mean across all covering requests: every fully covered
    # slice (or its split pieces, whose weights sum to 1) contributes
    # exactly weight 1.0.
    answers = {name: {"noul": sum_w[name] / cnt_w[name]} for name in sum_w}

    record = {
        "filename": str(f),
        "model": model,
        "fields": {
            name: {"p": round(answers[name]["noul"], 4), "true": answers[name]["noul"] >= args.min}
            for name in questions
        },
    }
    if args.verbose:
        record["input_tokens"] = input_tokens
        record["ms"] = int((time.monotonic() - t_file) * 1000)

    if out_fh:
        with (write_lock or threading.Lock()):
            out_fh.write(json.dumps(record) + "\n")
            out_fh.flush()

    true_fields = [n for n in questions if answers.get(n, {}).get("noul", 0) >= args.min]
    label = json.dumps(true_fields) if args.json else " ".join(true_fields)
    return record, answers, time.monotonic() - t_file, label




if __name__ == "__main__":
    main()
