import pandas as pd
import os

codigopath = os.getcwd()
path_inptu = codigopath + "/input"
path_output = codigopath + "/output"

done = pd.read_excel(path_inptu + "/archivos_consultados.xlsx")
path_historico = r"S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\FrameworkHistorico.csv"
historico = pd.read_csv(path_historico)

Directory = r"S:\BI\3. MERCHANDISING\FRAMEWORK\FRAMEWORK\2023"

os.chdir(Directory)
files = os.listdir()
files = [f for f in files if f[-4:] == 'xlsx']
# Seleccionando los archivos correctos ---------------------------------------------------------------------------------------------------
# if ~ are in the file name, it means that the file is not valid
files = [f for f in files if "~" not in f]
files = pd.DataFrame(files)
files.columns = ['name_file']
# get date created from file
files['date_created'] = files['name_file'].apply(lambda x: os.path.getmtime(x))

# transform date to datetime
files['date_created'] = pd.to_datetime(files['date_created'], unit='s')

# detect W in the file name
#files['W'] = files['name_file'].apply(lambda x: 'W' in x)

# detect W in the file name and substract 2 characters next to W
files['Week'] = files['name_file'].apply(lambda x: x[x.find('W')+1:x.find('W')+3] if 'W' in x else 'N')
files = files[files['Week'] != 'N']

files['Year'] = files['date_created'].apply(lambda x: x.year)

files = files.sort_values(by=['date_created'], ascending=False)

files.drop_duplicates(subset=['Week', 'Year'], keep='first', inplace=True)
files = files.sort_values(by=['date_created'], ascending=True)
files.reset_index(drop=True, inplace=True)

# Estos serán los archivos que se analizarán
# Eliminalos archivos que ya se analizaron
files = files[~files['name_file'].isin(done['name_file'])]
if len (files) == 0:
    print("No hay archivos nuevos")
    exit()

files
columnas_validas = pd.read_csv(path_inptu + "/ColumnasPestanas.csv")
pestanas_validas = columnas_validas["pestana"].unique()
# Comenzamos creando el dataframe definitivo ---------------------------------------------------------
col_definitivas = ["Week", 
"Year",	
"No.",  
"BU",
"BUFFER",
"CONF",
"VAN",
"VAN_CHINA",
"VAN_2_SAFE", 
"RES_LEAN",
"RES_MEDIO",
"RES_ALTO_VOL",	
"RES_TEMP"]


def detect_columns(file_excel):
    '''    Funcion que detecta las columnas de un archivo excel ''' 
    for i in range(len(file_excel.iloc[:,0].values)):
        if file_excel.iloc[i,0]=="Location Code":
            index = i
            break
    file_excel.columns = file_excel.iloc[index,:]
    file_excel = file_excel.iloc[index+1:,:]

    # Elimnando los nan de las columnas
    fc = file_excel.columns.to_list()
    fc = [x for x in fc if str(x) != 'nan']
    
    file_excel = file_excel[fc]
    file_excel.reset_index(drop=True, inplace=True)
    # Eliminando columnas repetidas dejando la primera
    #file_excel = file_excel.loc[:,~file_excel.columns.duplicated()]

    return file_excel

    
def seleccionar_columnas(file_excel, columnas_validas,sheet):
    '''   Selecciona las columnas validas  que se dieron de alta en el excel adjunto  '''
    file_excel = file_excel.loc[:,~file_excel.columns.duplicated()]

    columnas_validas_excel = columnas_validas[columnas_validas["pestana"]==sheet]
    columnas_validas_excel = columnas_validas_excel["columnas"].values

    file_excel_valido = pd.DataFrame()
    for col in file_excel.columns:
                
        try:
            if col in columnas_validas_excel:
                file_excel_valido[col] = file_excel[col]
        except:
            pass
    return file_excel_valido   

def cambiar_nombre_columnas(file_excel_valido, columnas_validas, sheet):
    
    '''Se cambia el nombre de las columnas por el cambio establecido en el excel'''
    
    cambiocol = []
    for i in range(len(file_excel_valido.columns)):
        if file_excel_valido.columns[i] in columnas_validas["columnas"].values:
            df = columnas_validas[columnas_validas["pestana"]==sheet]
            df = df[df["columnas"] == file_excel_valido.columns[i]]
            cambiocol.append(df["cambio"].values[0])
        else:
            cambiocol.append(file_excel_valido.columns[i])
    file_excel_valido.columns = cambiocol
    return file_excel_valido     

 
def datos_filename(file_excel_valido, file, sheet):

    ''' Se agregan los datos del nombre del archivo '''
    
    file_excel_valido["BU"] = sheet 
    file_excel_valido['Week'] = file[file.find('W')+1:file.find('W')+3] if 'W' in file else 'N'
    file_excel_valido["Year"] = file[file.find('202'):file.find('202')+4] if '20' in file else 'N'
    return file_excel_valido
    
def crear_categorias_acc(file_excel_valido):

    '''Se crean los titulos de las categorias de accesorios'''
    
    columns_acces = file_excel_valido.columns    

    BEAUTY = ["Location Code", 'Week', 'Year', 'CONF_BE']
    COMPLEMENTOS =  ["Location Code", 'Week', 'Year', 'CONF_NOBIS']
    BISUTERIA = ["Location Code", 'Week', 'Year', 'CONF_BIS']

    for name in columns_acces:
        if "BEA" in name:
            BEAUTY.append(name)
        elif "JEW" in name:
            BISUTERIA.append(name)
        elif "COMPLEMENTOS" in name:
            COMPLEMENTOS.append(name)    
        elif "ACC"  in name:
            COMPLEMENTOS.append(name) 
        elif "BISUTERIA" in name: 
            BISUTERIA.append(name)   
            
    dicc_categorias = {}
    for titulo in ['BEAUTY', 'COMPLEMENTOS', 'BISUTERIA']:
        if titulo == 'BEAUTY':
            dicc_categorias[titulo] = BEAUTY
        elif titulo == 'COMPLEMENTOS':
            dicc_categorias[titulo] = COMPLEMENTOS
        elif titulo == 'BISUTERIA':
            dicc_categorias[titulo] = BISUTERIA
    
    return dicc_categorias       


def detect_conf(file_excel, config, detect):
    '''Detecta la configuracion de la columna y la renombra'''
    for conf in config:
        if file_excel[conf].str[0:len(detect)].value_counts().index[0] == detect:
            file_excel[conf]
            #config.remove(conf)
            break

    return file_excel[conf]

#file = '221023 Framework de Tiendas 2022 W42.xlsx'
#sheet = 'CALZADO DAMA'
#sheet = 'ACCESORIOS'
#file = '221023 Framework de Tiendas 2022 W42.xlsx'
#file_excel = pd.read_excel(file, sheet_name=sheet)
#file_excel = detect_columns(file_excel)  

def crear_file_excel_valido_acces(file_excel, col_definitivas, file, sheet):
    # En acce existen varias columnas de configruacion lol
    # tenemos que seleccionar solo las que nos interesan
    # Se encuantran a la izquierda de  BISUTERIA_ COMPLEMENTOS_ BEAUTY_ no funciona pq se reordena lol 

    # Agregando un identificador a las columnas para poder eliminar las columnas repetidas

    file_excel.columns = [file_excel.columns[i] +'-'+ str(i) for i in range(len(file_excel.columns))]
    columns  = file_excel.columns

    # detect if CONF in column
    config = [x for x in columns if 'CONF' in x]

    CONF_BIS = detect_conf(file_excel, config, 'BIS')
    CONF_NOBIS = detect_conf(file_excel, config, 'NOB')
    CONF_BE = detect_conf(file_excel, config, 'BE')

    # Eliminar - y numero de columna
    file_excel.columns = [file_excel.columns[i].split('-')[0] for i in range(len(file_excel.columns))]

    file_excel_valido = seleccionar_columnas(file_excel, columnas_validas,sheet)

    file_excel_valido['CONF_BIS'] = CONF_BIS
    file_excel_valido['CONF_NOBIS'] = CONF_NOBIS
    file_excel_valido['CONF_BE'] = CONF_BE
    file_excel_valido = datos_filename(file_excel_valido, file, sheet)

    file_excel_valido_acces = file_excel_valido
    file_excel_valido = pd.DataFrame(columns=col_definitivas)

    for categoria in crear_categorias_acc(file_excel_valido_acces):
        name_columns_categ = crear_categorias_acc(file_excel_valido_acces)[categoria]
        df = file_excel_valido_acces[name_columns_categ]
        df = cambiar_nombre_columnas(df, columnas_validas, sheet)
        file_excel_valido = pd.concat([file_excel_valido, df], axis=0, ignore_index=True)
        
    file_excel_valido = datos_filename(file_excel_valido, file, sheet)
    
    file_excel_valido.loc[file_excel_valido['CONF'].str[0:3] == 'BIS', 'BU'] = 'BISUTERIA'
    file_excel_valido.loc[file_excel_valido['CONF'].str[0:3] == 'NOB', 'BU'] = 'COMPLEMENTOS'
    file_excel_valido.loc[file_excel_valido['CONF'].str[0:2] == 'BE', 'BU'] = 'BEAUTY'

    return file_excel_valido
  
df_definitivo_total = pd.DataFrame(columns=col_definitivas)

for file in files['name_file'].tolist():
    print("Analizando... ", file)
    
    for sheet in pestanas_validas:
        
        # Limpieza de datos ---------------------------------------------------------------
        file_excel = pd.read_excel(file, sheet_name=sheet)
        file_excel = detect_columns(file_excel)  
        
        # Cambio de columnas ----------------------------------------------------------------
        if sheet != "ACCESORIOS":
            file_excel_valido = seleccionar_columnas(file_excel, columnas_validas,sheet)  
            file_excel_valido = datos_filename(file_excel_valido, file, sheet)
            file_excel_valido = cambiar_nombre_columnas(file_excel_valido, columnas_validas, sheet)                 
            
        else:
            
            file_excel_valido = crear_file_excel_valido_acces(file_excel, col_definitivas, file, sheet)
            
        df_definitivo_total = pd.concat([df_definitivo_total, file_excel_valido], ignore_index=True)    
# detetcar el primer caracter de la columna No. y si no es T o E eliminar la fila
df_definitivo_total = df_definitivo_total[df_definitivo_total['No.'].str[0].isin(['T', 'E'])]
df_definitivo_total.sort_values(by=['Year', 'Week', 'No.', 'BU'], ascending=[False, False, True, True])

# Remplazando a nombres solicitados
df_definitivo_total['BU'].replace('ROPA DAMA', 'ROPA', inplace=True)
df_definitivo_total['BU'].replace('ROPA SH MAN', 'SH MAN', inplace=True)
df_definitivo_total['BU'].replace('CALZADO DAMA', 'CALZADO', inplace=True)
df_definitivo_total.to_excel(path_output + '/df_definitivo_total.xlsx', index=False)
historico = pd.concat([df_definitivo_total, historico], ignore_index=True)

historico = historico.sort_values(by=['Year', 'Week', 'No.', 'BU'], ascending=[False, False, True, True])
historico = historico.sort_values(by=['Year', 'Week', 'No.', 'BU'], ascending=[False, False, True, True])
historico.to_csv(path_historico, index=False)
files.sort_values(by=['Week'], ascending=[False], inplace=True)
files.to_excel(path_inptu+ '/files.xlsx', index=False)

done = pd.concat([done, files])
done.to_excel(path_inptu + "/archivos_consultados.xlsx" , index=False)

print('Proceso terminado')