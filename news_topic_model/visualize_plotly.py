

#!/usr/bin/env python3
"""
Plotly 3D visualizer for Spark clustering output.

Usage:
  python visualize_plotly.py <input_csv_or_folder> [--hide-outliers] [--export] [--html PATH] [--png PATH] [--title-len N]

Examples:
  python visualize_plotly.py out/tech_clusters_3d.csv --hide-outliers --export
  python visualize_plotly.py out/tech_clusters_3d.csv --html out/plot.html --png out/plot.png

Notes:
- Expects a CSV with columns: title,url,cluster,(optional)cluster_label,(optional)outlier,x,y,z
- If you pass the Spark output folder (e.g., out/tech_clusters_3d.csv/), the script will auto-find the part-*.csv
- If kaleido is installed, --export will also save a PNG in addition to HTML.
"""

import argparse
import glob
import os
import sys

try:
    import pandas as pd
except Exception as e:
    print("[error] pandas is required. Install with: conda install pandas (or pip install pandas)")
    raise

try:
    import plotly.express as px
    import plotly.io as pio
except Exception as e:
    print("[error] plotly is required. Install with: conda install -c plotly plotly (or pip install plotly)")
    raise


def _find_part_csv(path: str) -> str:
    """If path is a directory, return the first part-*.csv (excluding cluster_summary.csv). Otherwise return path."""
    if os.path.isdir(path):
        # Prefer the main coords CSV (not the cluster_summary.csv)
        candidates = sorted(
            [p for p in glob.glob(os.path.join(path, "*.csv")) if os.path.basename(p) != "cluster_summary.csv"],
        )
        if not candidates:
            raise FileNotFoundError(f"No CSV files found in folder: {path}")
        return candidates[0]
    return path


def _shorten(text: str, n: int) -> str:
    if not isinstance(text, str):
        return text
    return text if len(text) <= n else text[: max(0, n - 1)] + "…"


def load_data(input_path: str) -> pd.DataFrame:
    csv_path = _find_part_csv(input_path)
    df = pd.read_csv(csv_path)

    # Minimal sanitation
    for c in ("x", "y", "z"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if "outlier" not in df.columns:
        df["outlier"] = 0
    if "cluster_label" not in df.columns:
        # fall back to raw cluster id as label
        df["cluster_label"] = df["cluster"].astype(str)

    # Ensure cluster is categorical for stable legend ordering
    try:
        df["cluster"] = pd.to_numeric(df["cluster"], errors="coerce").astype("Int64")
    except Exception:
        pass

    return df


def make_figure(df: pd.DataFrame, hide_outliers: bool, title_len: int):
    if hide_outliers and "outlier" in df.columns:
        df = df[df["outlier"].fillna(0) == 0].copy()

    if title_len and "title" in df.columns:
        df["title"] = df["title"].map(lambda t: _shorten(t, title_len))

    color_col = "cluster_label" if "cluster_label" in df.columns else "cluster"

    # Build the figure
    fig = px.scatter_3d(
        df,
        x="x",
        y="y",
        z="z",
        color=color_col,
        hover_name="title" if "title" in df.columns else None,
        hover_data=[c for c in ["url", "cluster", "cluster_label", "outlier"] if c in df.columns],
        opacity=0.9,
    )

    # Styling tweaks
    fig.update_traces(marker=dict(size=5), selector=dict(mode="markers"))
    fig.update_layout(
        legend_title_text="Cluster",
        scene=dict(
            xaxis_title="PC 1",
            yaxis_title="PC 2",
            zaxis_title="PC 3",
        ),
        margin=dict(l=0, r=0, t=40, b=0),
        title="News clusters (3D)",
    )

    return fig


def export_figure(fig, html_path: str | None, png_path: str | None):
    wrote_any = False
    if html_path:
        pio.write_html(fig, file=html_path, auto_open=False, include_plotlyjs="cdn", full_html=True)
        print(f"[ok] Wrote HTML: {html_path}")
        wrote_any = True
    if png_path:
        try:
            pio.write_image(fig, png_path, scale=2, width=1200, height=800)
            print(f"[ok] Wrote PNG:  {png_path}")
            wrote_any = True
        except Exception as e:
            print(f"[warn] Could not save PNG (install kaleido): {e}")
    if not wrote_any:
        print("[info] No export paths provided; figure will just open in a browser window.")


def main():
    parser = argparse.ArgumentParser(description="Plotly 3D visualizer for Spark clustering output")
    parser.add_argument("input", help="CSV file or Spark CSV folder (e.g., out/tech_clusters_3d.csv)")
    parser.add_argument("--hide-outliers", action="store_true", help="Hide points with outlier==1")
    parser.add_argument("--export", action="store_true", help="Export HTML (and PNG if kaleido is installed)")
    parser.add_argument("--html", default=None, help="Path to write HTML (default: <input>/plot.html if --export)")
    parser.add_argument("--png", default=None, help="Path to write PNG (default: <input>/plot.png if --export)")
    parser.add_argument("--title-len", type=int, default=110, help="Truncate hover titles to N chars (default: 110)")

    args = parser.parse_args()

    df = load_data(args.input)
    fig = make_figure(df, hide_outliers=args.hide_outliers, title_len=args.title_len)

    # Default export paths if --export and not provided
    html_path = args.html
    png_path = args.png
    if args.export:
        base_dir = args.input if os.path.isdir(args.input) else os.path.dirname(os.path.abspath(args.input))
        if not html_path:
            html_path = os.path.join(base_dir, "plot.html")
        if not png_path:
            png_path = os.path.join(base_dir, "plot.png")

    if args.export or html_path or png_path:
        export_figure(fig, html_path, png_path)
    else:
        # Just show it
        fig.show()


if __name__ == "__main__":
    sys.exit(main())