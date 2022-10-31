from typing import Union
import json
import pandas as pd
from ipywidgets import widgets
from IPython.display import display
from .notebook import Notebook
from ._datavertex import ResourceProcessor
from sqlalchemy import Table, Boolean, Column, DateTime, Integer, Text, Float, JSON, Enum
import sqlalchemy

def get_sqlalchemy_type(type_string):
    if type_string == 'Bigint':
        return sqlalchemy.BigInteger()
    if type_string == 'Integer':
        return sqlalchemy.Integer()
    elif type_string == 'String':
        return sqlalchemy.String()
    elif type_string == 'Serial':
        return sqlalchemy.BigInteger()
    raise TypeError(f"{type_string} is not a valid data type.")

class ColumnSelection():
    
    def __init__(self, elements, dataset_name):
        self.elements = elements
        self.dataset_name = dataset_name
        
    def get(self):
        return {k: {"name": v[1].value, "datatype": v[2].value} for k, v in filter(lambda x: x[1][0].value, self.elements.items())}
    
    def persist(self):
        with open(f'datasets_interaction/{self.dataset_name}.json', 'wt') as f:
            json.dump(self.get(), f)
            
    def apply(self, df):
        selection = self.get()
        return df[selection.keys()].rename(columns={k: v['name'] for k, v in selection.items()})


class DataframeToDatabaseSync:

    def reload_table_data(self, engine: sqlalchemy.engine.Engine):
        print(f'Full loading table: {self.table_name} ({self.df.shape[0]} rows)')
        with engine.begin() as conn:
            conn.execute(f'delete from "{self.table_name}"')
            self.get_dataframe().to_sql(self.table_name, con=conn, index=False, if_exists='append', chunksize=1000, method='multi')
            print('Done')

    def get_dataframe(self):
        return self.selection.apply(self.df)

    def execute(self, engine: sqlalchemy.engine.Engine):
        
        metadata = sqlalchemy.MetaData()
        table = Table(self.table_name, metadata)

        for col, data in self.get_metadata().items():
            extra_args = {}
            if data['datatype'] == 'Serial' or data['datatype'] == 'UUID':
                extra_args['primary_key'] = True
            table.append_column(Column(data['name'], get_sqlalchemy_type(data['datatype']), **extra_args))

        print(f'Dropping table: {table.fullname}')
        table.drop(bind=engine, checkfirst=True)
        print(f'Creating table: {table.fullname}')
        table.create(bind=engine)
        print(f'Loading data: {table.fullname}')
        self.reload_table_data(engine) 

    def get_recommended_datatypes(series):
        datatype = series.dtype
        #print(datatype)
        if pd.core.dtypes.common.is_object_dtype(series):
            return ('String', ['String', 'UUID', 'Integer', 'Bigint', 'Float', 'Boolean', 'Datetime', 'Enum', 'Json'])
        elif pd.core.dtypes.common.is_datetime_or_timedelta_dtype(series):
            return ('Datetime', ['Datetime', 'String'])
        elif pd.core.dtypes.common.is_any_int_dtype(series):
            return ('Bigint', ['Serial', 'Integer', 'Bigint'])
        elif pd.core.dtypes.common.is_all_strings(series):
            return ('String', ['String'])
        elif pd.core.dtypes.common.is_float_dtype(series):
            return ('Float', ['Float', 'Bigint', 'Integer', 'String'])
        elif pd.core.dtypes.common.is_bool_dtype(series):
            return ('Boolean', ['Boolean', 'Integer', 'String'])
        else:
            raise KeyError(series.dtype)

    def select_columns(self, df, dataset_name):
        rows = {}
        
        selection = {c: c for c in df.columns}
        
        try:
            with open(f'datasets_interaction/{dataset_name}.json', 'rt') as f:
                selection = json.load(f)
        except FileNotFoundError as e:
            pass
        
        for col in df.columns:
            checkbox = widgets.Checkbox(value=col in selection, description=col)
            
            column_new_name = selection[col]['name'] if col in selection and isinstance(selection[col], dict) and 'name' in selection[col] else col
            
            # For combatibility
            if col in selection and isinstance(selection[col], str):
                column_new_name = selection[col]
            
            text = widgets.Text(value=column_new_name, placeholder=col, description='', disabled=False)
            
            datatype_label = widgets.Label(value=str(df[col].dtype) + " to")
            
            recommended_datatype, datatypes_choice = DataframeToDatabaseSync.get_recommended_datatypes(df[col])
            selected_datatype = selection[col]['datatype'] if col in selection and isinstance(selection[col], dict) and 'datatype' in selection[col] else recommended_datatype
            
            if selected_datatype not in datatypes_choice:
                incompatible_type = selected_datatype + " (Incompatible)"
                selected_datatype = incompatible_type
                datatypes_choice.append(incompatible_type)

            
            datatype = widgets.Dropdown(
                options=datatypes_choice, #['String', 'Integer', 'Bigint', 'Float', 'Boolean', 'Datetime', 'Enum', 'Json'],
                value=selected_datatype, #'String',
                description='',
                disabled=False,
                )
            row = widgets.HBox([checkbox, text, datatype_label, datatype])
            display(row)
            rows[col] = (checkbox, text, datatype)
        
        return ColumnSelection(rows, dataset_name)

    def __init__(self, nb: Notebook, obj: Union[str, pd.DataFrame, ResourceProcessor], table_name: str):

        self.table_name = table_name
        self.nb = nb

        if isinstance(obj, pd.DataFrame):
            df = obj
        elif isinstance(obj, str):
            df = nb.inputs[obj].read_dataframe()
        elif isinstance(obj, ResourceProcessor):
            df = obj.read_dataframe()
        display(df)

        self.df = df
        self.selection = self.select_columns(df, table_name)
        
    def get_metadata(self):
        return self.selection.get()
    
    def get_df(self):
        return self.df