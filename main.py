"""
Googleスプレッドシートから「D列(氏名)」と「翌日日付の列」を抽出し、
書式(背景色・文字色)を維持したHTMLテーブルを生成 → Playwrightで画像化 → LINE送信。

地域(kanto / kansai / kyushu)ごとにスプレッドシート・送信先を切り替える。

必要な環境変数:
  SPREADSHEET_ID_KANTO   - 関東シートのスプレッドシートID
  SPREADSHEET_ID_KANSAI  - 関西シートのスプレッドシートID
  SPREADSHEET_ID_KYUSHU  - 九州シートのスプレッドシートID
  GOOGLE_API_KEY         - Google Cloud で発行した APIキー (Sheets API 有効化済み)
  LINE_CHANNEL_TOKEN     - LINE Messaging APIのチャネルアクセストークン (全地域共通)
  LINE_TARGET_ID_KANTO   - 関東の送信先ID
  LINE_TARGET_ID_KANSAI  - 関西の送信先ID
  LINE_TARGET_ID_KYUSHU  - 九州の送信先ID
  IMAGE_PUBLIC_URL_BASE  - screenshots/ ディレクトリの公開URL (末尾スラッシュ省略可)

前提: スプレッドシートを「リンクを知っている全員 (閲覧者)」で共有しておく。
      APIキー方式では非公開シートは読めない。
"""

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

# 地域ごとの設定。
#   name_col_index        : 氏名列のインデックス (0-based)。A列=0, D列=3
#   terminator            : 氏名にこの文字列を含む行で打ち切る。不要なら None
#   trim_trailing_empty   : True の場合、末尾の空行(氏名も日付も空)を削除
REGIONS = {
    "kanto": {
        "label": "関東",
        "spreadsheet_id_env": "SPREADSHEET_ID_KANTO",
        "line_target_env": "LINE_TARGET_ID_KANTO",
        "terminator": "アクア",
        "name_col_index": 3,
        "trim_trailing_empty": False,
    },
    "kansai": {
        "label": "関西",
        "spreadsheet_id_env": "SPREADSHEET_ID_KANSAI",
        "line_target_env": "LINE_TARGET_ID_KANSAI",
        "terminator": None,
        "name_col_index": 3,
        "trim_trailing_empty": True,
    },
    "kyushu": {
        "label": "九州",
        "spreadsheet_id_env": "SPREADSHEET_ID_KYUSHU",
        "line_target_env": "LINE_TARGET_ID_KYUSHU",
        "terminator": None,
        "name_col_index": 0,
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
    # TODO: 検証後に戻す
    return datetime.date(2026, 4, 26)


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


def find_target(sheets, target_date):
    """翌日日付を含む (sheet_index, header_row_index, col_index) を返す"""
    for si, sheet in enumerate(sheets):
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


# ------------------------- 画像化 & 送信 -------------------------

def render_screenshot(region):
    if read_status(region).get("all_off"):
        print(f"[{region}] 全員休みのためスクリーンショット生成をスキップ", file=sys.stderr)
        return
    src = html_path(region).resolve().as_uri()
    dst = screenshot_path(region)
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            viewport={"width": 1200, "height": 800},
            device_scale_factor=2,
        )
        page = context.new_page()
        page.goto(src)
        page.wait_for_load_state("networkidle")
        page.locator(".container").screenshot(path=str(dst))
        browser.close()
    print(f"[{region}] Saved: {dst}", file=sys.stderr)


def send_line_image(region):
    config = REGIONS[region]
    label = config["label"]
    token = os.environ["LINE_CHANNEL_TOKEN"]
    to_id = os.environ[config["line_target_env"]]
    all_off = read_status(region).get("all_off", False)

    if all_off:
        messages = [
            {
                "type": "text",
                "text": (
                    f"お疲れ様です。\n"
                    f"明日の{label}は全員休みのため、シフト表の配信を省略します。"
                ),
            }
        ]
        mode = "全員休み通知"
    else:
        base_url = os.environ["IMAGE_PUBLIC_URL_BASE"].rstrip("/")
        image_url = f"{base_url}/latest_{region}.png"
        messages = [
            {
                "type": "text",
                "text": f"お疲れ様です。\n明日の{label}の店舗情報になります。\nご確認よろしくお願いいたします。",
            },
            {
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url,
            },
        ]
        mode = "画像配信"

    resp = requests.post(
        "https://api.line.me/v2/bot/message/push",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"to": to_id, "messages": messages},
        timeout=30,
    )
    resp.raise_for_status()
    print(f"[{region}] LINE push OK ({mode})", file=sys.stderr)


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


def _looks_like_member(name_cell):
    """氏名欄が実メンバー(役職カッコ付き)かを判定。見出し/注記行を除外する"""
    name = (name_cell or {}).get("formattedValue", "") or ""
    if not name.strip():
        return False
    return ("(" in name) or ("（" in name)


def _all_members_off(rows):
    """実メンバー行について、全員のシフトが休みマーカー or 空白かを判定"""
    member_rows = [
        (name_cell, date_cell)
        for name_cell, date_cell in rows
        if _looks_like_member(name_cell)
    ]
    if not member_rows:
        return False
    return all(_cell_is_off(date_cell) for _, date_cell in member_rows)


def build_table(region):
    """HTMLを生成してファイル保存。翌日列が見つからなければ TargetDateNotFound を送出"""
    config = REGIONS[region]
    terminator = config["terminator"]
    name_col_index = config["name_col_index"]

    service = build_sheets_service()
    spreadsheet_id = os.environ[config["spreadsheet_id_env"]]
    target = tomorrow_jst()

    data = fetch_sheets(service, spreadsheet_id)
    sheets = data.get("sheets", [])
    found = find_target(sheets, target)
    if not found:
        raise TargetDateNotFound(f"[{region}] 翌日 {target} を含む列が見つかりませんでした")

    si, header_ri, ci = found
    sheet = sheets[si]
    title = sheet["properties"]["title"]
    row_data = sheet["data"][0].get("rowData", [])

    rows = []
    for row in row_data[header_ri + 1:]:
        values = row.get("values", []) or []
        name_cell = values[name_col_index] if name_col_index < len(values) else None
        date_cell = values[ci] if ci < len(values) else None
        name_text = (name_cell or {}).get("formattedValue", "") or ""
        if terminator and terminator in name_text:
            break
        rows.append((name_cell, date_cell))

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


def run_all(phase):
    """全地域を順次実行。1地域が失敗しても他地域は継続し、最後に失敗があれば exit 1。

    phase: 'build_render' -> build_table + render_screenshot
           'send'         -> send_line_image
    """
    failed = []
    for region in REGIONS:
        try:
            if phase == "build_render":
                build_table(region)
                render_screenshot(region)
            elif phase == "send":
                send_line_image(region)
            else:
                raise ValueError(f"unknown phase: {phase}")
        except Exception:
            traceback.print_exc()
            failed.append(region)
    if failed:
        print(f"Failed regions ({phase}): {failed}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    run_all("build_render")
    run_all("send")
