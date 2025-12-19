import calendar
from pathlib import Path
from typing import List

import pandas as pd
import panel as pn

from helioscta_python.helioscta_python.utils import (
    logging_utils,
    azure_postgresql,
    panel_utils,
)

from helioscta_python.helioscta_python.chunk_storage.v1_2025_dec_19 import (
    configs,
    pandas_azure_writer,
    utils,
)

# Create a global logger instance
logger = logging_utils.init_logging(
    name = "TERM_BIBLE",
    log_to_file = True,
    delete_if_no_errors = True,
)

"""
"""

## =======================================================
## =======================================================

def _pull_from_sql(sql_filename: str = configs.filename) -> pd.DataFrame:
    """"""
    
    sql_file_path = Path(__file__).parent.parent / "sql" / f'{sql_filename}.sql'

    with open(sql_file_path, 'r') as f:
        query = f.read()
    
    df = azure_postgresql.pull_from_db(query=query)
    logger.info(f"Pulled {len(df)} rows from {sql_filename} ...")
    
    return df


def _basic_write(
        writer, 
        df: pd.DataFrame,
        blob_name = configs.azure_blob_name,
    ):
    
    azure_blob_url = writer.write_parquet(
        df=df,
        blob_name=f'{blob_name}.parquet',
        engine="pyarrow",
        compression="snappy",
    )
    logger.info(f"Wrote {len(df)} to {azure_blob_url} ...")


def _basic_read(
        writer,
        blob_name = configs.azure_blob_name,
    ):
    
    # Read specific partition
    try:
        df = writer.read_parquet(
            blob_name=f'{blob_name}.parquet',
        )
        logger.info(f"Read {len(df)} from {blob_name} ...")
        return df
    except ValueError as e:
        logger.error(f"Error reading {blob_name}: {e}")


def pull():
    # df = _pull_from_sql()
    
    # # Initialize writer

    # # test basic write
    # writer = pandas_azure_writer.PandasAzureWriter()
    # _basic_write(
    #     writer=writer,
    #     df=df,
    # )    

    # test basic write
    writer = pandas_azure_writer.PandasAzureWriter()
    df = _basic_read(
        writer=writer,
    )

    return df 

## =======================================================
## =======================================================

def get_term_bible(
        df: pd.DataFrame,
        year_col: str = 'year',
        month_col: str = 'month',
        value_col: str = 'hh_cash',
        aggfunc='mean',
    ) -> pd.DataFrame:
    
    term_bible = df.pivot_table(
        index=year_col,
        columns=month_col,
        values=value_col,
        aggfunc=aggfunc,
    )

    # Formatting
    term_bible = term_bible.round(3)
    # months
    term_bible.columns = [calendar.month_abbr[int(col)] for col in term_bible.columns]
    # years
    term_bible = term_bible.reset_index()
    term_bible['year'] = term_bible['year'].astype(int).astype(str)

    return term_bible


def get_monthly_stats(
        df: pd.DataFrame,
        year_col: str = 'year',
        month_col: str = 'month',
        value_col: str = 'hh_cash',
        stats: List[str] = ['mean', 'min', 'max'],
    ) -> pd.DataFrame:
    
    # First group by year_month to get monthly averages
    monthly_avg = df.groupby([year_col, month_col])[value_col].mean()

    # Then get mean, min, max of monthly values by NOTE: month
    monthly_stats = monthly_avg.groupby(month_col).agg(stats).T.round(2)

    # formatting
    monthly_stats.columns = [calendar.month_abbr[int(col)] for col in monthly_stats.columns]
    monthly_stats = monthly_stats.reset_index().rename(columns={'index': 'stat'})
    monthly_stats['stat'] = monthly_stats['stat'].str.capitalize()

    return monthly_stats


def get_yearly_stats(
        df: pd.DataFrame,
        year_col: str = 'year',
        month_col: str = 'month',
        value_col: str = 'hh_cash',
        stats: List[str] = ['mean', 'min', 'max'],
    ) -> pd.DataFrame:
    
    # First group by year and month to get monthly averages
    monthly_avg = df.groupby([year_col, month_col])[value_col].mean()

    # Then get mean, min, max of monthly values by NOTE: year
    yearly_stats = monthly_avg.groupby(year_col).agg(stats).round(2)
    
    # formatting
    yearly_stats = yearly_stats.reset_index()
    yearly_stats[year_col] = yearly_stats[year_col].astype(int).astype(str)

    return yearly_stats

## =======================================================
## =======================================================

def generate_artifacts(
        df: pd.DataFrame,
        term_bible: pd.DataFrame,
    ) -> dict:

    table_term_bible = utils.get_styled_term_bible(term_bible=term_bible)

    artifacts: dict = {
        "df": df,
        "table_term_bible": table_term_bible,
    }

    return artifacts

"""
"""


if __name__ == '__main__':
    
    df = pull()

    # term bible
    value_col = 'hh_cash'
    term_bible = get_term_bible(
        df=df,
        value_col=value_col,
    )

    # artifacts
    artifacts: dict = generate_artifacts(
        df=df,
        term_bible=term_bible,
    )

    title = f"Term Bible"
    panel_builder = panel_utils.PanelDashboardBuilder(title=title)
        
    combined_content = pn.Column(
        panel_utils._section_header(f"Term Bible"),
        pn.Row(
            pn.Column(
                artifacts["table_term_bible"],
                # artifacts["table_monthly_stats"],
                # sizing_mode="stretch_width",
                # max_width=500,
            ),
            # pn.Column(
            #     artifacts["table_yearly_stats"],
            #     # sizing_mode="stretch_width",
            #     # max_width=400,
            # ),
            sizing_mode="stretch_width",
        ),
        # panel_utils._section_header(f"Daily Values"),
        # pn.Row(
        #     pn.Column(
        #         artifacts["table_daily_values"],
        #         sizing_mode="stretch_width",
        #         max_width=600,
        #     ),
        #     pn.pane.Plotly(artifacts["fig_daily_values"], height=1000, sizing_mode="stretch_width"),
        # ),
        sizing_mode="stretch_width",
    )
    
    panel_builder.add_tab("Term Bible", combined_content)
    panel_builder.save('term_bible.html')