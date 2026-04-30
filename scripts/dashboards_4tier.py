from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COIN = ROOT / "monitoring" / "grafana" / "dashboards" / "coin.json"
MAIN = ROOT / "monitoring" / "grafana" / "dashboards" / "main.json"


ID_BANNER_DESC = 200
ID_BANNER_DIAG = 201
ID_BANNER_PRED = 202
ID_BANNER_RX   = 203
ID_DIVERGENCE  = 210
ID_FORECAST_15 = 220
ID_FORECAST_60 = 221
ID_FORECAST_240 = 222
ID_PREDICT_HISTORY = 223
ID_SIGNAL_STAT = 230
ID_SCORE_GAUGE = 231
ID_REASONS_TABLE = 232
ID_SIGNAL_HISTORY = 233
ID_MAIN_SIGNAL_TABLE = 240


def infinity_target(refid: str, url_path: str, columns: list[dict] | None = None,
                    is_table: bool = True) -> dict:
    """V22: parser=backend on all Infinity targets."""
    t = {
        "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity-velora"},
        "refId": refid,
        "type": "json",
        "source": "url",
        "format": "table" if is_table else "timeseries",
        "url": f"http://host.docker.internal:8000{url_path}",
        "url_options": {"data": "", "method": "GET"},
        "parser": "backend",
        "root_selector": "",
        "filters": [],
    }
    if columns is not None:
        t["columns"] = columns
    return t


def stat_panel(pid: int, x: int, y: int, w: int, h: int, title: str, target: dict,
               unit: str = "short", color: str = "blue") -> dict:
    return {
        "id": pid,
        "type": "stat",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity-velora"},
        "targets": [target],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": [
                    {"color": color, "value": None},
                ]},
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
            "textMode": "auto",
            "colorMode": "value",
            "orientation": "auto",
        },
    }


def text_banner(pid: int, x: int, y: int, w: int, h: int, content: str, title: str = "") -> dict:
    return {
        "id": pid,
        "type": "text",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "options": {"mode": "markdown", "content": content},
    }


def row_panel(pid: int, y: int, title: str, collapsed: bool = False) -> dict:
    return {
        "id": pid,
        "type": "row",
        "title": title,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "collapsed": collapsed,
        "panels": [],
    }


def timeseries_panel(pid: int, x: int, y: int, w: int, h: int, title: str,
                     targets: list[dict], unit: str = "short") -> dict:
    return {
        "id": pid,
        "type": "timeseries",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity-velora"},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 10,
                           "showPoints": "never", "pointSize": 5},
                "color": {"mode": "palette-classic"},
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi"},
        },
    }


def gauge_panel(pid: int, x: int, y: int, w: int, h: int, title: str, target: dict,
                vmin: float = -1.0, vmax: float = 1.0) -> dict:
    return {
        "id": pid,
        "type": "gauge",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity-velora"},
        "targets": [target],
        "fieldConfig": {
            "defaults": {
                "min": vmin, "max": vmax,
                "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": [
                    {"color": "red",    "value": None},
                    {"color": "yellow", "value": -0.4},
                    {"color": "green",  "value":  0.4},
                ]},
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"values": False, "calcs": ["lastNotNull"], "fields": ""},
            "showThresholdLabels": False,
            "showThresholdMarkers": True,
        },
    }


def table_panel(pid: int, x: int, y: int, w: int, h: int, title: str, target: dict) -> dict:
    return {
        "id": pid,
        "type": "table",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "datasource": {"type": "yesoreyeram-infinity-datasource", "uid": "infinity-velora"},
        "targets": [target],
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "displayMode": "auto"}}, "overrides": []},
        "options": {"showHeader": True, "footer": {"show": False}},
    }


def upsert_panel(panels: list[dict], new_panel: dict) -> bool:
    """Append if id not present. Returns True if added."""
    pid = new_panel["id"]
    for p in panels:
        if p.get("id") == pid:
            return False
    panels.append(new_panel)
    return True


def patch_coin(d: dict) -> int:
    panels = d["panels"]
    added = 0

    for p in panels:
        if p.get("id") == 100:
            p["title"] = "🟦 DESCRIPTIVE — current snapshot"
        elif p.get("id") == 101:
            p["title"] = "🟨 DIAGNOSTIC — why (lag correlation)"
        elif p.get("id") == 102:
            p["title"] = "🟦 DESCRIPTIVE — market action"
        elif p.get("id") == 105:
            p["title"] = "🟦 DESCRIPTIVE — recent tweets"

    max_y = max((p.get("gridPos", {}).get("y", 0) + p.get("gridPos", {}).get("h", 0))
                for p in panels)

    diag_y = max_y
    diag_target_a = infinity_target("A", "/coin/${coin}/divergence?days=7", is_table=False)
    diag_target_a["columns"] = [
        {"selector": "ts", "text": "ts", "type": "timestamp"},
        {"selector": "v_twitter", "text": "v_twitter", "type": "number"},
        {"selector": "v_bluesky", "text": "v_bluesky", "type": "number"},
        {"selector": "divergence", "text": "divergence", "type": "number"},
    ]
    if upsert_panel(panels, row_panel(204, diag_y, "🟨 DIAGNOSTIC — cross-source divergence")):
        added += 1
    if upsert_panel(panels, timeseries_panel(
        ID_DIVERGENCE, 0, diag_y + 1, 24, 8,
        "Cross-source sentiment divergence (twitter vs bluesky)",
        [diag_target_a], unit="short")):
        added += 1

    pred_y = diag_y + 9
    if upsert_panel(panels, row_panel(ID_BANNER_PRED, pred_y, "🟧 PREDICTIVE — short-horizon return forecast")):
        added += 1

    for i, h in enumerate([15, 60, 240]):
        pid = 220 + i
        col = ["green", "blue", "purple"][i]
        target = infinity_target(f"P{i}", f"/coin/${{coin}}/predict?horizon={h}")
        target["columns"] = [
            {"selector": "predicted_return_pct", "text": "value", "type": "number"},
        ]
        if upsert_panel(panels, stat_panel(
            pid, i*8, pred_y + 1, 8, 5,
            f"{h}-min predicted return %", target, unit="percent", color=col)):
            added += 1

    pred_hist_target = infinity_target("PH", "/coin/${coin}/predict-history?horizon=60&days=7", is_table=False)
    pred_hist_target["columns"] = [
        {"selector": "ts", "text": "ts", "type": "timestamp"},
        {"selector": "predicted_return_pct", "text": "predicted_return_pct", "type": "number"},
        {"selector": "confidence", "text": "confidence", "type": "number"},
    ]
    if upsert_panel(panels, timeseries_panel(
        ID_PREDICT_HISTORY, 0, pred_y + 6, 24, 9,
        "60-min forecast history (last 7d) + confidence",
        [pred_hist_target], unit="percent")):
        added += 1

    rx_y = pred_y + 15
    if upsert_panel(panels, row_panel(ID_BANNER_RX, rx_y, "🟥 PRESCRIPTIVE — recommended action")):
        added += 1

    sig_target = infinity_target("S", "/coin/${coin}/recommend")
    sig_target["columns"] = [{"selector": "signal", "text": "signal", "type": "string"}]
    sig_panel = stat_panel(ID_SIGNAL_STAT, 0, rx_y + 1, 8, 6,
                           "Signal", sig_target, unit="none", color="text")
    sig_panel["fieldConfig"]["defaults"]["mappings"] = [
        {"type": "value", "options": {
            "buy":  {"color": "green",  "index": 0, "text": "BUY ▲"},
            "sell": {"color": "red",    "index": 1, "text": "SELL ▼"},
            "hold": {"color": "yellow", "index": 2, "text": "HOLD —"},
        }},
    ]
    if upsert_panel(panels, sig_panel):
        added += 1

    score_target = infinity_target("G", "/coin/${coin}/recommend")
    score_target["columns"] = [{"selector": "score", "text": "score", "type": "number"}]
    if upsert_panel(panels, gauge_panel(
        ID_SCORE_GAUGE, 8, rx_y + 1, 8, 6,
        "Score (-1 = sell ▼ | +1 = buy ▲)", score_target, vmin=-1, vmax=1)):
        added += 1

    reasons_target = infinity_target("R", "/coin/${coin}/recommend")
    reasons_target["root_selector"] = "reasons"
    reasons_target["columns"] = [
        {"selector": "feature",      "text": "feature",      "type": "string"},
        {"selector": "value",        "text": "value",        "type": "number"},
        {"selector": "sign",         "text": "sign",         "type": "number"},
        {"selector": "contribution", "text": "contribution", "type": "number"},
        {"selector": "note",         "text": "note",         "type": "string"},
    ]
    if upsert_panel(panels, table_panel(
        ID_REASONS_TABLE, 16, rx_y + 1, 8, 6,
        "Reasons (V51)", reasons_target)):
        added += 1

    sig_hist_target = infinity_target("SH", "/coin/${coin}/recommend-history?days=7", is_table=False)
    sig_hist_target["columns"] = [
        {"selector": "ts",     "text": "ts",     "type": "timestamp"},
        {"selector": "score",  "text": "score",  "type": "number"},
    ]
    if upsert_panel(panels, timeseries_panel(
        ID_SIGNAL_HISTORY, 0, rx_y + 7, 24, 7,
        "Signal score history (last 7d)", [sig_hist_target], unit="short")):
        added += 1

    return added


def patch_main(d: dict) -> int:
    """Mutate main.json. Add Prescriptive row + signal heatmap table."""
    panels = d["panels"]
    added = 0

    max_y = max((p.get("gridPos", {}).get("y", 0) + p.get("gridPos", {}).get("h", 0))
                for p in panels)

    rx_y = max_y
    if upsert_panel(panels, row_panel(203, rx_y, "🟥 PRESCRIPTIVE — coin signals")):
        added += 1

    sig_target = infinity_target("R", "/coin/${coin:value}/recommend")
    sig_target["columns"] = [
        {"selector": "signal", "text": "signal", "type": "string"},
        {"selector": "score",  "text": "score",  "type": "number"},
    ]
    pdef = stat_panel(ID_MAIN_SIGNAL_TABLE, 0, rx_y + 1, 4, 5,
                       "${coin} signal", sig_target, unit="none", color="text")
    pdef["fieldConfig"]["defaults"]["mappings"] = [
        {"type": "value", "options": {
            "buy":  {"color": "green",  "index": 0, "text": "BUY ▲"},
            "sell": {"color": "red",    "index": 1, "text": "SELL ▼"},
            "hold": {"color": "yellow", "index": 2, "text": "HOLD —"},
        }},
    ]
    pdef["repeat"] = "coin_each"
    pdef["repeatDirection"] = "h"
    if upsert_panel(panels, pdef):
        added += 1

    templating = d.setdefault("templating", {"list": []})
    var_list = templating.setdefault("list", [])
    has_each = any(v.get("name") == "coin_each" for v in var_list)
    if not has_each:
        var_list.append({
            "name": "coin_each",
            "type": "custom",
            "label": "coin_each",
            "hide": 2,
            "multi": True,
            "includeAll": True,
            "current": {"text": ["bitcoin","ethereum","solana","ripple","binance","dogecoin"],
                        "value": ["bitcoin","ethereum","solana","ripple","binance","dogecoin"]},
            "options": [
                {"text": c, "value": c, "selected": True}
                for c in ["bitcoin","ethereum","solana","ripple","binance","dogecoin"]
            ],
            "query": "bitcoin,ethereum,solana,ripple,binance,dogecoin",
        })
        added += 1

    return added


def main() -> None:
    coin = json.loads(COIN.read_text(encoding="utf-8"))
    n_coin = patch_coin(coin)
    COIN.write_text(json.dumps(coin, indent=2), encoding="utf-8")
    print(f"coin.json: {n_coin} panels added")

    main_d = json.loads(MAIN.read_text(encoding="utf-8"))
    n_main = patch_main(main_d)
    MAIN.write_text(json.dumps(main_d, indent=2), encoding="utf-8")
    print(f"main.json: {n_main} elements added")


if __name__ == "__main__":
    main()
