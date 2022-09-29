import pandas as pd
import os

Directory = r"C:\Users\fcolin\Desktop\Todos"
os.chdir(Directory)
files = os.listdir()
files = [f for f in files if f[-4:] == 'xlsx']

sheets_aceptadas  = ["ROPA DAMA", "SHASA MAN", "CALZADO", "BISUTERIA", "NO BISUTERIA", "BEAUTY"]

sheets_aceptadas_conversion = ["ROPA", "SH MAN","CALZADO", "BISUTERIA", "COMPLEMENTOS", "BEAUTY"]

df_colnames = pd.read_csv("hist_colnames2.csv")

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

# create a dataframe with the columns

df_definitivo_total = pd.DataFrame(columns=col_definitivas)
df_definitivo_total.to_csv("df_definitivo_total.csv", index=False)


#k = 0 
# '190103 Framework de Clasificación de Tiendas 2019 S01.xlsx'
for k in range(len(files)-1):
    file = files[k]
    print("Analizando... ", file)

    pestanas = pd.ExcelFile(file).sheet_names

    pestanas_aceptadas = []
    for pestana in pestanas: 
        if pestana in sheets_aceptadas:
            pestanas_aceptadas.append(pestana)


    #s = 4
    for s in range(len(pestanas_aceptadas)-1):

        df_definitivo = pd.DataFrame(columns=col_definitivas)

        # reading excel file no ommited rows and create a new column name
        file_excel = pd.read_excel(file, sheet_name=pestanas_aceptadas[s], header=None)

        print("En: " + pestanas_aceptadas[s])

        for i in range(len(file_excel.iloc[:,0].values)):
            if file_excel.iloc[i,0]=="No.":
                index = i
                break

        col_names = file_excel.iloc[index].values

        for i in range(len(col_names)):
            name = col_names[i]
            if name in df_colnames["col_names_validos"].values:
                # show the change
                cambio = df_colnames[df_colnames["col_names_validos"]==name]["col_names_cambio"].values[0]
                # replace the name
                col_names[i] = cambio

        file_excel.iloc[index] = col_names        

        coincidences = []
        print("Procesando... ")
        for i in range(len(file_excel.iloc[index].values)):
            col = file_excel.iloc[index,i]
            if col in col_definitivas:
                if col not in coincidences:
                    coincidences.append(col)
                    df_definitivo[col] = (file_excel.iloc[index+1:,i].values)
        


        filas = len(df_definitivo["Week"])
        semana = file[len(file)-7:len(file)-5]
        # add semana at colum week
        df_definitivo["Week"] = [semana]*filas

        anio = "20"+file[:2]
        df_definitivo["Year"] = [anio]*filas

        if pestanas_aceptadas[s] in sheets_aceptadas:
            pestana_conversion = sheets_aceptadas_conversion[sheets_aceptadas.index(pestanas_aceptadas[s])]
        
        df_definitivo["BU"] = [pestana_conversion]*filas

    
        df_definitivo_total = pd.read_csv("df_definitivo_total.csv")
        df_definitivo_total = pd.concat([df_definitivo_total, df_definitivo], ignore_index=True)
        df_definitivo_total.to_csv("df_definitivo_total.csv", index=False)
        

        del df_definitivo