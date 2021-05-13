import pandas as pd

filepath = "Tables_Batch4.csv"

df = pd.read_csv(filepath)

# DF PREPROCESSING
for c in df.columns:
    if df.dtypes[c] not in ['int64',]:
        df[c] = df[c].str.strip()


sap_sql_map_dict = {
    'ACCP': 'NVARCHAR', 
    'CHAR': 'NVARCHAR', 
    'CLNT': 'NVARCHAR', 
    'CUKY': 'NVARCHAR', 
    'CURR': 'DECIMAL', 
    'DEC': 'DECIMAL', 
    'DATS': 'NVARCHAR', 
    'FLTP': 'FLOAT', 
    'INT1': 'SMALLINT', 
    'INT2': 'INT', 
    'INT4': 'BIGINT', 
    'LANG': 'NVARCHAR', 
    'NUMC': 'NVARCHAR', 
    'QUAN': 'DECIMAL', 
    'RAW': 'VARBINARY', 
    'TIMS': 'NVARCHAR', 
    'UNIT': 'NVARCHAR',
    
    'PREC': 'NVARCHAR',
    'VARC': 'NVARCHAR',
    'LRAW': 'NVARCHAR',
    'LCHR': 'NVARCHAR',
    'STRING': 'NVARCHAR',
    'RAWSTRING': 'NVARCHAR',
}



def create_table(table_name):
    tdf = df[df['TABNAME'] == table_name]
    tdf = tdf.sort_values(by=['POSITION'], ascending=True)
    tdf['DATATYPE_SQL'] = tdf['DATATYPE'].map(sap_sql_map_dict)
    
    
    def fieldname_replace(row):
        """
        Reformat fieldname if it begins with '\'.
        """
        if row['FIELDNAME'][0] == '\\':
            fieldname_sql = "[{}]".format(row['FIELDNAME'])
        else:
            fieldname_sql = row['FIELDNAME']
        
        return fieldname_sql
    
    tdf['FIELDNAME_SQL'] = tdf.apply(lambda row: fieldname_replace(row), axis=1)    
    
    
    def full_field_str_configure(row):
        """
        Configure datatype-specific params:
            if decimal -> fieldname DECIMAL(LENG, DECIMALS) (ex: UKURS DECIMAL(9,5),)
            elif SMALLINT, INT, BIGINT -> fieldname dataype
            otherwise -> fieldname dataype(LENG)
        """

        if row['DATATYPE_SQL'] == 'DECIMAL':
            full_field_str = "{} {}({},{})".format(row['FIELDNAME_SQL'], row['DATATYPE_SQL'], row['LENG'], row['DECIMALS'])
        elif row['DATATYPE_SQL'] in ['SMALLINT', 'INT', 'BIGINT']:
            full_field_str = "{} {}".format(row['FIELDNAME_SQL'], row['DATATYPE_SQL'])
        else:
            full_field_str = "{} {}({})".format(row['FIELDNAME_SQL'], row['DATATYPE_SQL'], row['LENG'])
        
        return full_field_str
            
    tdf['FIELD_SQL_FULL'] = tdf.apply(lambda row: full_field_str_configure(row), axis=1)
    
    
    fields_sql = tdf['FIELD_SQL_FULL'].tolist()
    fields_sql_str_full = ""
    for f in range(len(fields_sql)):
        if f == len(fields_sql)-1: # if it's the last fieldname in the list, don't include the newline. Also, don't include comma if len(pks) == 0 ?
            fields_sql_str_full += "\t{},".format(fields_sql[f])
        else:
            fields_sql_str_full += "\t{},\n".format(fields_sql[f])
    
    
    pks = tdf[tdf['KEYFLAG'] == 'X']['FIELDNAME_SQL'].tolist()
    pks_str = ",".join(pks)
    
    if len(pks) == 0:
        pks_str_full = ""
    else:
        pks_str_full = "\tCONSTRAINT PK_{TABLE_NAME} PRIMARY KEY CLUSTERED ({PKS_STR})".format(TABLE_NAME=table_name, PKS_STR=pks_str)

    
    create_table_full_str = """
    Create Table S4.{TABLE_NAME} (
    {FIELDS_SQL}
    {PKS_STR_FULL}
    )
    GO
    """.format(TABLE_NAME=table_name, FIELDS_SQL=fields_sql_str_full, PKS_STR_FULL=pks_str_full)
    
    return create_table_full_str



tables_list = df['TABNAME'].unique()
print(tables_list)

for t in tables_list:
    create_table_txt = create_table(t)
    print(create_table_txt)
