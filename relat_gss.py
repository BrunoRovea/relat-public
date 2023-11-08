from GSSLibs import log, TSWS, Relat
import pandas as pd
from datetime import datetime, timedelta
 
log.setup("ex_TSWS_alarmes_Relat", ".")
tsws = TSWS.setup('./config.json')
relat = Relat.setup('config.json')
 
# Defina os parâmetros da busca
start_time = datetime.now() - timedelta(hours=24)
end_time = datetime.now()
search_pattern = "*SUPERV/CONTR UN COMANDO DE PARADA ACIONADO*"

# Realize a busca de alarmes
response = tsws.get.time_series_events(search_pattern, start_time, end_time)
timeSeriesResponse = response.json().get("timeSeriesResponse")

if not timeSeriesResponse:
    raise Exception("Não foi retornado nenhum evento")

alarmes = pd.DataFrame(timeSeriesResponse)

# Filtragem e manipulação
filtro = alarmes['value'].str.contains('successful control')
alarmes_filtrados = alarmes[filtro]
alarmes_filtrados['time_stamp'] = pd.to_datetime(alarmes_filtrados['timestamp'], format="%Y-%m-%dT%H:%M:%S")
alarmes_filtrados['unidade'] = alarmes_filtrados['value'].str.extract(r'(U\d{2})')[0]

# Criação do DataFrame df_unid_veloc
tag_unid_veloc = []
for i in range(1, 19):
    tag_unidade = f'U{i:02d}'
    nova_string = f'{tag_unidade}     .REG_VELOC VELOC UNID {"MAIOR/IGUAL" if i >= 10 else "MAYOR/IGUAL"} 80% V.NOM      .SV'
    tag_unid_veloc.append((tag_unidade, nova_string))

df_unid_veloc = pd.DataFrame(tag_unid_veloc, columns=['tag_unidades', 'tag_unid_veloc'])

# Mesclagem de DataFrames
df_parad_veloc = alarmes_filtrados.merge(df_unid_veloc, left_on='unidade', right_on='tag_unidades', how='inner')

# # Função de consulta
# def minha_funcao(row):
#     response = tsws.get.time_series_interpolate(row['tag_unid_veloc'], row['time_stamp'])
#     return response.json().get('timeSeriesResponse', {}).get('value', None)

# df_parad_veloc['status'] = df_parad_veloc.apply(minha_funcao, axis=1)
# df_parad_veloc = df_parad_veloc[df_parad_veloc['status']=='SIM']

## Criando Ocorrência
ocorrencia = relat.models.ocorrenciaOPU(
    # TimestampOcorrencia=df_parad_veloc['time_stamp'].iloc[0],
    TimestampOcorrencia=datetime.now(),
    TipoOcorrencia_DescricaoPT="Ocorrência Automática",
    DescricaoOcorrencia='Parada da unidade '+df_parad_veloc['unidade'].iloc[0]
    )

## Cadastrando Ocorrência
cadastro = relat.insert.cadastrarOcorrenciaOPU(ocorrencia)
print(cadastro.text, "\n\n")