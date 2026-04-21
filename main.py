"""
Googleスプレッドシートから「D列(氏名)」と「翌日日付の列」を抽出し、
書式(背景色・文字色)を維持したHTMLテーブルを生成 → Playwrightで画像化 → LINE送信。

必要な環境変数:
  SPREADSHEET_ID     - スプレッドシートID
  GOOGLE_API_KEY     - Google Cloud で発行した APIキー (Sheets API 有効化済み)
  LINE_CHANNEL_TOKEN - LINE Messaging APIのチャネルアクセストークン
  LINE_TARGET_ID     - 送信先のユーザー/グループ/ルームID
  IMAGE_PUBLIC_URL   - 画像が公開されるURL (raw.githubusercontent.com 等)

前提: スプレッドシートを「リンクを知っている全員 (閲覧者)」で共有しておく。
      APIキー方式では非公開シートは読めない。
"""

import datetime
import html
import os
import re
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from googleapiclient.discovery import build
from playwright.sync_api import sync_playwright

OUT_DIR = Path("screenshots")
HTML_PATH = OUT_DIR / "latest.html"
SCREENSHOT_PATH = OUT_DIR / "latest.png"

NAME_COL_INDEX = 3       # D列 (0-based)
HEADER_SCAN_ROWS = 3     # 日付ヘッダーを探す行数 (上から)


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
        "     margin:24px;background:#fff;color:#222;}",
        "h2{margin:0 0 14px 0;font-size:20px;}",
        "table{border-collapse:collapse;font-size:15px;box-shadow:0 1px 3px rgba(0,0,0,.1);}",
        "th,td{border:1px solid #888;padding:10px 16px;text-align:center;min-width:90px;}",
        "th{background:#f5f5f5;font-weight:bold;}",
        "</style></head><body>",
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
    lines.append("</table></body></html>")
    return "\n".join(lines)


# ------------------------- 画像化 & 送信 -------------------------

def render_screenshot():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context(
            viewport={"width": 1200, "height": 800},
            device_scale_factor=2,
        )
        page = context.new_page()
        page.goto(HTML_PATH.resolve().as_uri())
        page.wait_for_load_state("networkidle")
        page.locator("body").screenshot(path=str(SCREENSHOT_PATH))
        browser.close()
    print(f"Saved: {SCREENSHOT_PATH}", file=sys.stderr)


def send_line_image():
    token = os.environ["LINE_CHANNEL_TOKEN"]
    to_id = os.environ["LINE_TARGET_ID"]
    image_url = os.environ["IMAGE_PUBLIC_URL"]
    resp = requests.post(
        "https://api.line.me/v2/bot/message/push",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "to": to_id,
            "messages": [{
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url,
            }],
        },
        timeout=30,
    )
    resp.raise_for_status()
    print("LINE push OK", file=sys.stderr)


# ------------------------- メイン -------------------------

def build_table():
    """HTMLを生成してファイル保存。翌日列が見つからなければ非0で終了"""
    service = build_sheets_service()
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    target = tomorrow_jst()

    data = fetch_sheets(service, spreadsheet_id)
    sheets = data.get("sheets", [])
    found = find_target(sheets, target)
    if not found:
        print(f"翌日 {target} を含む列が見つかりませんでした", file=sys.stderr)
        sys.exit(1)

    si, header_ri, ci = found
    sheet = sheets[si]
    title = sheet["properties"]["title"]
    row_data = sheet["data"][0].get("rowData", [])

    rows = []
    for row in row_data[header_ri + 1:]:
        values = row.get("values", []) or []
        name_cell = values[NAME_COL_INDEX] if NAME_COL_INDEX < len(values) else None
        date_cell = values[ci] if ci < len(values) else None
        # 氏名が空の行はスキップ
        if not name_cell or not (name_cell.get("formattedValue") or "").strip():
            continue
        rows.append((name_cell, date_cell))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    HTML_PATH.write_text(build_html(title, target, rows), encoding="utf-8")
    print(f"Sheet: {title} / col={ci} / rows={len(rows)}", file=sys.stderr)


if __name__ == "__main__":
    build_table()
    render_screenshot()
    send_line_image()
