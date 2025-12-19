import os
import calendar
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd
import panel as pn

"""
"""

# Year color constants for consistent plotting
YEAR_COLORS = {
    '2026': '#FF4500',  # Orange Red
    '2025': '#8B0000',  # Dark Red
    '2024': '#333333',  # Dark Grey
    '2023': '#00008B',  # Dark Blue
    '2022': '#9370DB',  # Medium Orchid
    '2021': '#87CEEB',  # Sky Blue
    '2020': '#90EE90',  # Light Green
    '2019': '#FFD700',  # Gold
    '2018': '#FF6347',  # Tomato
    '2017': '#DDA0DD',  # Plum
    '2016': '#20B2AA',  # Light Sea Green
    '2015': '#F0E68C',  # Khaki
    '2014': '#CD853F',  # Peru
    '2013': '#4682B4',  # Steel Blue
    '2012': '#DA70D6',  # Orchid
    '2011': '#32CD32',  # Lime Green
    '2010': '#FF69B4',  # Hot Pink
}

## =====================================
## =====================================

def _stylesheet():
    """Create CSS stylesheet to color specific columns orange"""
    css_rules = []

    # Row and cell hover highlighting
    # Crosshair highlight styles
    css_rules.append('''
        .tabulator-row:hover {
            background-color: #e6f2ff !important;
        }
        .tabulator-cell:hover {
            background-color: #cce5ff !important;
        }
    ''')

    # default header color
    css_rules.append('''
        .tabulator .tabulator-header,
        .tabulator .tabulator-header .tabulator-col,
        .tabulator .tabulator-header .tabulator-col-content {
            background-color: #D3D3D3 !important;
        }
    ''')


def _apply_red_green_gradient_style(
    series: pd.Series,
    threshold_high_min: float = None,  # q75
    threshold_high_max: float = None,  # vmax
    threshold_low_min: float = None,   # q25
    threshold_low_max: float = None,   # vmin
    color_high_min: str = "#66BB6A",   # Light green (at q75)
    color_high_max: str = "#1B5E20",   # Dark green (at vmax)
    color_low_min: str = "#EF5350",    # Light red (at q25)
    color_low_max: str = "#B71C1C",    # Dark red (at vmin)
) -> pd.Series:
    """Return CSS background color strings for a series."""
    
    def _hex_to_rgb(hex_color):
        hex_color = hex_color.lstrip("#")
        return np.array([int(hex_color[i:i+2], 16) for i in (0, 2, 4)])
    
    def _get_color(v):
        try:
            v = float(v)
        except (ValueError, TypeError):
            return "background-color: white"
        
        if pd.isna(v):
            return "background-color: white"
        
        if v >= threshold_high_min:
            # Green gradient (q75 to vmax)
            t = np.clip((v - threshold_high_min) / (threshold_high_max - threshold_high_min), 0, 1)
            c_low = _hex_to_rgb(color_high_min)
            c_high = _hex_to_rgb(color_high_max)
            rgb = (1 - t) * c_low + t * c_high
            text_color = "white" if t > 0.5 else "black"
        elif v <= threshold_low_min:
            # Red gradient (q25 to vmin)
            t = np.clip((v - threshold_low_min) / (threshold_low_max - threshold_low_min), 0, 1)
            c_low = _hex_to_rgb(color_low_min)
            c_high = _hex_to_rgb(color_low_max)
            rgb = (1 - t) * c_low + t * c_high
            text_color = "white" if t > 0.5 else "black"
        else:
            # Middle values (between q25 and q75) - white
            return "background-color: white; color: black"
        
        rgb = np.clip(rgb, 0, 255).astype(int)
        return f"background-color: #{rgb[0]:02X}{rgb[1]:02X}{rgb[2]:02X}; color: {text_color}"
    
    return series.apply(_get_color)

## =====================================
## =====================================


def _style_term_bible(df):
    """Apply red-green gradient styling to term bible pivot table."""
    
    # Get numeric columns only (exclude 'year')
    numeric_cols = [col for col in df.columns if col != 'year']
    
    # Get min/max/quartiles across all numeric values
    all_values = df[numeric_cols].values.flatten()
    all_values = all_values[~np.isnan(all_values)]
    
    threshold_low_max = np.min(all_values)
    threshold_low_min = np.percentile(all_values, 25)
    threshold_high_min = np.percentile(all_values, 75)
    threshold_high_max = np.max(all_values)
    
    styles = pd.DataFrame('', index=df.index, columns=df.columns)
    
    for col in numeric_cols:
        styles[col] = _apply_red_green_gradient_style(
            series=df[col],
            threshold_high_min=threshold_high_min,
            threshold_high_max=threshold_high_max,
            threshold_low_min=threshold_low_min,
            threshold_low_max=threshold_low_max,
        )
    
    return (
        df.style
        .apply(lambda _: styles, axis=None)
        .set_properties(**{'font-weight': 'bold'})
    )


def get_styled_term_bible(term_bible: pd.DataFrame):

        columns = term_bible.columns.to_list()

        styled_df = _style_term_bible(term_bible)
        stylesheet = _stylesheet()

        # create table
        pn.extension('tabulator')
        table = pn.widgets.Tabulator(
            styled_df,
            show_index=False,
            theme='simple',
            header_filters=False,
            disabled=True,
            widths={col: 65 for col in columns},  # Set default width for all columns
            configuration={
                'columnDefaults': {
                    'hozAlign': 'center',
                    'headerHozAlign': 'center',
                },  
                'columns': [
                    {'field': col, 'headerSort': False, 'title': '' if col == 'year' else col} for col in columns
                ],
            },
            stylesheets=[stylesheet],
        )

        return table