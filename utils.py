import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
from google.oauth2.service_account import Credentials
import os
import logging


def write_dataframe_to_sheet(df: pd.DataFrame, sheetname, tabname):
    """
    Writes a pandas DataFrame to the given Google Sheet worksheet.
    It will clear the sheet first, then write the entire DataFrame.
    
    Args:
        df (pd.DataFrame): The DataFrame to write.
        sheetname: str: The name of the Google Sheet.
        tabname: str: The name of the worksheet/tab in the Google Sheet.
    """
    try:
        scope = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        # read in google credentials from json file
        # get google_creds_path variable from .env file
        credentials_path = os.getenv("GOOGLE_CREDS_PATH")
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open(sheetname).worksheet(tabname)
        sheet.clear()
        set_with_dataframe(sheet, df)
        logger.info(f"✅ Successfully wrote DataFrame to {sheetname} - {tabname}.")
    except Exception as e:
        logger.error(f"❌ Error writing DataFrame to Google Sheet: {e}")