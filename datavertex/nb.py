from IPython.display import display, Markdown
import pandas as pd
from .notebook import Notebook

__all__ = ['md', 'preview', 'display', 'Notebook']

def md(markdown):
    display(Markdown(markdown))

def preview(df):
    max_rows = pd.get_option('display.max_rows')
    pd.set_option('display.max_rows', 5)
    display(df)
    pd.set_option('display.max_rows', max_rows)