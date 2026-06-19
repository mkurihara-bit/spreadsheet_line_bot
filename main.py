"""
Googleスプレッドシートから「D列(氏名)」と「翌日日付の列」を抽出し、
書式(背景色・文字色)を維持したHTMLテーブルを生成 → Playwrightで画像化
→ GASウェブアプリ経由で共有ドライブに保存。

地域(kanto / kansai / kyushu)ごとにスプレッドシートを切り替え、
画像はそれぞれ「YYYY-MM-DD_地域名.png」という名前で保存される。
保存後の配信(LINE等)は人が手動で行う想定。

必要な環境変数:
  SPREADSHEET_ID_KANTO   - 関東シートのスプレッドシートID
  SPREADSHEET_ID_KANSAI  - 関西シートのスプレッドシートID
  SPREADSHEET_ID_KYUSHU  - 九州シートのスプレッドシートID
  GOOGLE_API_KEY         - Google Cloud で発行した APIキー (Sheets API 有効化済み)
  GAS_WEBAPP_URL         - 画像保存用 GAS ウェブアプリの /exec URL
  GAS_TOKEN              - GAS と一致させる合言葉トークン

前提: スプレッドシートを「リンクを知っている全員 (閲覧者)」で共有しておく。
      APIキー方式では非公開シートは読めない。
"""

import base64
import datetime
import html
import json
import os
import re
import sys
import traceback
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from googleapiclient.discovery import build
from playwright.sync_api import sync_playwright

OUT_DIR = Path("screenshots")

HEADER_SCAN_ROWS = 3     # 日付ヘッダーを探す行数 (上から)

# 氏名欄がこれらの値の場合「休み」と判定する (大文字小文字はそのまま比較)
OFF_MARKERS = {"休", "休み", "×", "x", "X", "OFF", "off"}

# 氏名セルの判定パターン。(店長)/(CL候補)/(キャッチ) を含むセルを氏名として扱う。
# 全角(）半角() の両方に対応。
ROLE_PATTERN = re.compile(r"[（(](店長|CL候補|キャッチ)[）)]")

# 地域ごとの設定。
#   terminator            : 氏名セルにこの文字列を含む行で打ち切る。不要なら None
#   trim_trailing_empty   : True の場合、末尾の空行(氏名も日付も空)を削除
REGIONS = {
    "kanto": {
        "label": "関東",
        "spreadsheet_id_env": "SPREADSHEET_ID_KANTO",
        "terminator": "アクア",
        "trim_trailing_empty": False,
    },
    "kansai": {
        "label": "関西",
        "spreadsheet_id_env": "SPREADSHEET_ID_KANSAI",
        "terminator": None,
        "trim_trailing_empty": True,
    },
    "kyushu": {
        "label": "九州",
        "spreadsheet_id_env": "SPREADSHEET_ID_KYUSHU",
        "terminator": None,
        "trim_trailing_empty": True,
    },
}


class TargetDateNotFound(Exception):
    """翌日の日付列がシート内に見つからなかった場合"""


def html_path(region):
    return OUT_DIR / f"latest_{region}.html"


def screenshot_path(region):
    return OUT_DIR / f"latest_{region}.png"


def status_path(region):
    return OUT_DIR / f"status_{region}.json"


def read_status(region):
    path = status_path(region)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


# ------------------------- Sheets サービス -------------------------

def build_sheets_service():
    api_key = os.environ["GOOGLE_API_KEY"]
    return build("sheets", "v4", developerKey=api_key, cache_discovery=False)


# ------------------------- 日付ユーティリティ -------------------------

def tomorrow_jst():
    now = datetime.datetime.now(ZoneInfo("Asia/Tokyo"))
    return (now + datetime.timedelta(days=1)).date()


def serial_to_date(serial):
    """Google Sheets のシリアル値 -> date (1899-12-30 起点)"""
    try:
        return datetime.date(1899, 12, 30) + datetime.timedelta(days=int(serial))
    except (ValueError, OverflowError):
        return None


def parse_date_text(text, default_year):
    """よくある日本語シートの日付表記をパース"""
    text = (text or "").strip()
    if not text:
        return None
    patterns = [
        (r"^(\d{4})[/\-年](\d{1,2})[/\-月](\d{1,2})日?$", lambda m: (int(m[1]), int(m[2]), int(m[3]))),
        (r"^(\d{1,2})[/\-](\d{1,2})", lambda m: (default_year, int(m[1]), int(m[2]))),
        (r"^(\d{1,2})月(\d{1,2})日", lambda m: (default_year, int(m[1]), int(m[2]))),
    ]
    for pat, extract in patterns:
        m = re.match(pat, text)
        if m:
            try:
                y, mo, d = extract(m)
                return datetime.date(y, mo, d)
            except ValueError:
                continue
    return None


def cell_matches_date(cell, target):
    if not cell:
        return False
    ev = cell.get("effectiveValue") or {}
    if "numberValue" in ev:
        d = serial_to_date(ev["numberValue"])
        if d == target:
            return True
    parsed = parse_date_text(cell.get("formattedValue", ""), target.year)
    return parsed == target


# ------------------------- Sheets API -------------------------

def fetch_sheets(service, spreadsheet_id):
    fields = (
        "sheets(properties(title),"
        "data(rowData(values(effectiveValue,formattedValue,"
        "effectiveFormat(backgroundColor,textFormat)))))"
    )
    return service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        includeGridData=True,
        fields=fields,
    ).execute()


def find_target(sheets, target_date, skip_title_substr=None):
    """翌日日付を含む (sheet_index, header_row_index, col_index) を返す。

    skip_title_substr: タブ名にこの文字列を含むシートは走査対象から除外する。
        関東の「アクア」タブのように、同一スプレッドシート内の別チーム用タブを
        誤って拾わないために使う。
    """
    for si, sheet in enumerate(sheets):
        title = sheet.get("properties", {}).get("title", "") or ""
        if skip_title_substr and skip_title_substr in title:
            continue
        data_arr = sheet.get("data") or []
        if not data_arr:
            continue
        row_data = data_arr[0].get("rowData") or []
        for ri in range(min(HEADER_SCAN_ROWS, len(row_data))):
            for ci, cell in enumerate(row_data[ri].get("values", []) or []):
                if cell_matches_date(cell, target_date):
                    return si, ri, ci
    return None


# ------------------------- HTML 生成 -------------------------

def color_css(color):
    if not color:
        return None
    r = int(round(color.get("red", 0) * 255))
    g = int(round(color.get("green", 0) * 255))
    b = int(round(color.get("blue", 0) * 255))
    return f"rgb({r},{g},{b})"


def cell_style(cell):
    if not cell:
        return "background-color:#ffffff"
    fmt = cell.get("effectiveFormat") or {}
    parts = []
    bg = color_css(fmt.get("backgroundColor")) or "#ffffff"
    parts.append(f"background-color:{bg}")
    tf = fmt.get("textFormat") or {}
    fg = color_css(tf.get("foregroundColor"))
    if fg:
        parts.append(f"color:{fg}")
    if tf.get("bold"):
        parts.append("font-weight:bold")
    if tf.get("italic"):
        parts.append("font-style:italic")
    size = tf.get("fontSize")
    if size:
        parts.append(f"font-size:{size}px")
    return ";".join(parts)


def build_html(sheet_title, target_date, rows):
    header_date = target_date.strftime("%Y/%m/%d (%a)")
    lines = [
        "<!DOCTYPE html>",
        "<html lang='ja'><head><meta charset='utf-8'>",
        "<style>",
        "body{font-family:'Yu Gothic','Hiragino Sans','Noto Sans JP',sans-serif;",
        "     margin:0;background:#fff;color:#222;}",
        ".container{display:inline-block;padding:16px;}",
        "h2{margin:0 0 14px 0;font-size:20px;}",
        "table{border-collapse:collapse;font-size:15px;box-shadow:0 1px 3px rgba(0,0,0,.1);}",
        "th,td{border:1px solid #888;padding:10px 16px;text-align:center;min-width:90px;}",
        "th{background:#f5f5f5;font-weight:bold;}",
        "</style></head><body>",
        "<div class='container'>",
        f"<h2>{html.escape(sheet_title)} — {header_date} のシフト</h2>",
        "<table>",
        f"<tr><th>氏名</th><th>{header_date}</th></tr>",
    ]
    for name_cell, date_cell in rows:
        name_text = (name_cell or {}).get("formattedValue", "") or ""
        date_text = (date_cell or {}).get("formattedValue", "") or ""
        lines.append(
            "<tr>"
            f"<td style='{cell_style(name_cell)}'>{html.escape(name_text)}</td>"
            f"<td style='{cell_style(date_cell)}'>{html.escape(date_text)}</td>"
            "</tr>"
        )
    lines.append("</table></div></body></html>")
    return "\n".join(lines)


# ------------------------- 画像化 & アップロード -------------------------

def render_screenshot(region):
    src = html_path(region).resolve().as_uri()
    dst = screenshot_path(region)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        # device_scale_factor=1: 画像ファイルサイズを抑制
        context = browser.new_context(
            viewport={"width": 1200, "height": 800},
            device_scale_factor=1,
        )
        page = context.new_page()
        page.goto(src)
        page.wait_for_load_state("networkidle")
        page.locator(".container").screenshot(path=str(dst))
        browser.close()
    print(f"[{region}] Saved: {dst}", file=sys.stderr)


def upload_to_gas():
    """生成済みの画像を GAS ウェブアプリへ送信し、共有ドライブに保存する。

    stale(古い)画像を誤って送らないよう、status の target_date が「明日」と
    一致する地域だけを送信対象にする。生成に失敗した地域は前回の画像が
    残っていても target_date が古いままなので自動的に除外される。
    """
    url = os.environ["GAS_WEBAPP_URL"]
    token = os.environ["GAS_TOKEN"]
    target = tomorrow_jst()

    images = []
    for region, config in REGIONS.items():
        status = read_status(region)
        if status.get("target_date") != target.isoformat():
            print(f"[{region}] 今回未更新のため送信対象外", file=sys.stderr)
            continue
        path = screenshot_path(region)
        if not path.exists():
            print(f"[{region}] 画像が無いため送信スキップ", file=sys.stderr)
            continue
        data = base64.b64encode(path.read_bytes()).decode("ascii")
        name = f"{target.isoformat()}_{config['label']}.png"  # 例: 2026-06-19_関東.png
        images.append({"name": name, "data": data})

    if not images:
        raise RuntimeError("送信できる画像がありません(全地域が生成失敗の可能性)")

    resp = requests.post(
        url,
        json={"token": token, "images": images},
        timeout=120,
    )
    resp.raise_for_status()
    result = resp.json()
    if not result.get("ok"):
        raise RuntimeError(f"GAS がエラーを返しました: {result}")
    print(
        f"GAS upload OK: saved={result.get('saved')} deleted={result.get('deleted')}",
        file=sys.stderr,
    )


# ------------------------- メイン -------------------------

def _row_is_empty(row):
    name_cell, date_cell = row
    name_text = (name_cell or {}).get("formattedValue", "") or ""
    date_text = (date_cell or {}).get("formattedValue", "") or ""
    return not name_text.strip() and not date_text.strip()


def _cell_is_off(date_cell):
    """シフト値が休みマーカー or 空白なら休みと判定"""
    if not date_cell:
        return True
    text = (date_cell.get("formattedValue") or "").strip()
    if not text:
        return True
    return text in OFF_MARKERS


def find_name_cell_in_row(values, exclude_col):
    """行内で『氏名(店長)/(CL候補)/(キャッチ)』のセルを返す。
    役職ラベルだけのセル「(店長)」「(キャッチ)」(役職ヘッダー)は除外する。"""
    for i, cell in enumerate(values):
        if i == exclude_col:
            continue
        text = (cell or {}).get("formattedValue", "") or ""
        m = ROLE_PATTERN.search(text)
        if m is None:
            continue
        # 役職ラベル前に氏名(非空白文字)がある場合のみ氏名セルとして採用
        if text[:m.start()].strip():
            return cell
    return None


def is_role_header_row(values, exclude_col):
    """行内に『(店長)』『(キャッチ)』など氏名なしの役職ラベル単体セルがあるか判定"""
    for i, cell in enumerate(values):
        if i == exclude_col:
            continue
        text = (cell or {}).get("formattedValue", "") or ""
        m = ROLE_PATTERN.search(text)
        if m is None:
            continue
        if not text[:m.start()].strip():
            return True
    return False


def _all_members_off(rows):
    """全員のシフトが休みマーカー or 空白かを判定 (rows は既にメンバー行のみ)"""
    if not rows:
        return False
    return all(_cell_is_off(date_cell) for _, date_cell in rows)


def build_table(region):
    """HTMLを生成してファイル保存。翌日列が見つからなければ TargetDateNotFound を送出"""
    config = REGIONS[region]
    terminator = config["terminator"]

    service = build_sheets_service()
    spreadsheet_id = os.environ[config["spreadsheet_id_env"]]
    target = tomorrow_jst()

    data = fetch_sheets(service, spreadsheet_id)
    sheets = data.get("sheets", [])
    found = find_target(sheets, target, skip_title_substr=terminator)
    if not found:
        raise TargetDateNotFound(f"[{region}] 翌日 {target} を含む列が見つかりませんでした")

    si, header_ri, ci = found
    sheet = sheets[si]
    title = sheet["properties"]["title"]
    row_data = sheet["data"][0].get("rowData", [])

    rows = []
    member_started = False  # 最初の氏名行を見つけたら True (それ以前のメモ書きは無視)
    for row in row_data[header_ri + 1:]:
        values = row.get("values", []) or []
        date_cell = values[ci] if ci < len(values) else None
        name_cell = find_name_cell_in_row(values, exclude_col=ci)

        if name_cell is not None:
            name_text = name_cell.get("formattedValue", "") or ""
            if terminator and terminator in name_text:
                break
            rows.append((name_cell, date_cell))
            member_started = True
            continue

        # 役職ヘッダー単体行(『(店長)』『(キャッチ)』のみ)は除外
        if is_role_header_row(values, exclude_col=ci):
            continue

        # 氏名行以降の住所/メモ書き行は氏名セル空で含める(日付セルが空の行は除外)
        if member_started:
            date_text = (date_cell or {}).get("formattedValue", "") or ""
            if date_text.strip():
                rows.append((None, date_cell))

    if config["trim_trailing_empty"]:
        while rows and _row_is_empty(rows[-1]):
            rows.pop()

    all_off = _all_members_off(rows)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    html_path(region).write_text(build_html(title, target, rows), encoding="utf-8")
    status_path(region).write_text(
        json.dumps({"all_off": all_off, "target_date": target.isoformat()}),
        encoding="utf-8",
    )
    print(
        f"[{region}] Sheet: {title} / col={ci} / rows={len(rows)} / all_off={all_off}",
        file=sys.stderr,
    )


def build_all():
    """全地域の画像を生成。1地域が失敗しても他は継続し、失敗地域のリストを返す。"""
    failed = []
    for region in REGIONS:
        try:
            build_table(region)
            render_screenshot(region)
        except Exception:
            traceback.print_exc()
            failed.append(region)
    return failed


if __name__ == "__main__":
    failed = build_all()
    # 生成できた地域の画像を共有ドライブへ保存（古い画像は送らない仕組み）
    upload_to_gas()
    if failed:
        print(f"Failed regions: {failed}", file=sys.stderr)
        sys.exit(1)
