
# Json do estado disjuntor 18A ñ funcionando
# https://pi-api-vm01/piwebapi/attributes?path=\\PI-SIRI\SF6_50  .VANO_U9A INT 85U9A ABIERTO                      .SV
# https://pi-api-vm01/piwebapi/attributes?path=\\PI-SIRI\SF6_50  .VANO_U18A INT 86U8A ABIERTO                     .SV



# import sys
# sys.path.append(os.path.join(os.path.dirname(__file__), "libs_opso"))
import os

from client import websocket_client
from GSSLibs import log, Relat
import collections
import dateutil.parser as dparser
import signal
import asyncio
import json
import base64
import requests
from datetime import datetime

# Executar no jupyter notebook funções assíncronas
import nest_asyncio
nest_asyncio.apply()

# implement ring buffer
ring_buffer = collections.deque(maxlen=100)

def try_parsing_date(text):
    '''
    Try to parse a date from a string
    
    Return:
        datetime.datetime: if the date is valid otherwise None
    '''
    try:
        return dparser.parse(text, fuzzy=True).replace(tzinfo=None)
    except Exception as e:
        log.info(f"Error parsing date: {e}")
        return None


def flatten_data(y):
    '''
    Flatten a nested dictionary https://stackoverflow.com/questions/51359783/how-to-flatten-multilevel-nested-json
    
    Return:
        dict: flatten dictionary
    '''
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def read_webids(path):
    '''
    Read webids from a json file
    
    Return:
        list: list of webids
    '''
    with open(path, "r") as f:
        webids = json.load(f)
    # find all webids
    webids = flatten_data(webids)
    webids = [v for k, v in webids.items() if "webid" in k]
    # remove duplicates
    webids = list(set(webids))
    webids = [webid for webid in webids if webid != ""]
    webids_chunks = list(array_chunks(webids))

    webids = ["webId=" + "&webId=".join(webid) for webid in webids_chunks]
    return webids


def array_chunks(array, chunk_size=50):
    for i in range(0, len(array), chunk_size):
        yield array[i:i + chunk_size]


async def message_callback(message):
    '''
    Send a request to the webhook for each webid and timestamp received from the websocket
    '''
    global ring_buffer
    
    resp = json.loads(message)

    log.info(f"Recieve message from websocket: {resp}")

    # Log na rede pi d novo
    user = ''
    password = ''
    auth = user + ':' + password
    authHeader = 'Basic ' + base64.b64encode(auth.encode('utf-8')).decode('utf-8')
    headers = {'Authorization': authHeader}



    timestamp = try_parsing_date(resp['Items'][0]['Items'][0]['Timestamp'])
    if timestamp is None:
        log.info(f"Error parsing timestamp: {resp['Items'][0]['Items'][0]['Timestamp']} is not a valid date format. WebId: {resp['Items'][0]['Items']}")

    # O comando de parada pode ter comutado e estar em não

    # No caso da webid teste, o valor de comutação é igual a 2
    # No comanod de parada deve-se testar para os valores 'SI' ou 'SIM'

    if resp['Items'][0]['Items'][0]['Value'] == 2:
    # if resp['Items'][0]['Items'][0]['Value']['Name'] == 'SIM' or 'SI':
    
        # Abre o arquivo estado_equipamentos novamente
        with open('estado_equipamentos.json', 'r') as file:
            # Cria o json equipamentos do arquivo
            equipamentos = json.load(file)
        

        # Itera o arquivo equipamentos
        for equipamento in equipamentos:
            # Procura a webid que mudou de estado no arquivo aberto agora
            # Procura qual máquina obteve o comando de parada, ou comando de partida
            if equipamento['webid'] == resp['Items'][0]['WebId']:
                # Gera o link para obtenção do estado do ponto de velocidade ou estado da seccionadora
                # Necessariamente o primeiro children
                url_01 = 'https://pi-api-vm01.siri.itaipu/piwebapi/streams/' + equipamento['children'][0]['webid'] + '/value'
                response_01 = requests.get(url_01, headers=headers, verify=False)

                # Detecta se houve algum erro na obtenção da resposta
                if response_01.status_code != 200:
                    log.error(response_01.text)
                    # Em caso falso, segue 
                    continue

                # Gera o link para a obtenção do disjuntor da unidade geradora
                # Necessariamente o segundo children
                url_02 = 'https://pi-api-vm01.siri.itaipu/piwebapi/streams/' + equipamento['children'][1]['webid'] + '/value'
                response_02 = requests.get(url_02, headers=headers, verify=False)


                # Detecta se houve algum erro na obtenção da resposta
                if response_02.status_code != 200:
                    log.error(response_02.text)
                    # Em caso falso, segue 
                    continue


                # Gera o link para obtenção do estado do disjuntor de meio
                # Necessariamente o segundo link
                url_03 = 'https://pi-api-vm01.siri.itaipu/piwebapi/streams/' + equipamento['children'][2]['webid'] + '/value'
                response_03 = requests.get(url_03, headers=headers, verify=False)

                # Detecta se houve algum erro na obtenção da resposta
                if response_03.status_code != 200:
                    log.error(response_03.text)
                    # Em caso falso, segue 
                    continue
                
                # Verifica se o tipo de ocorrência comutado é um comando de parada
                if equipamento['Tipo Ocorrencia'] == 'Comando de Parada':
                    # VErifica se a unidade geradora está com velocidade nominal > 80%
                    if "SI" or "SIM" in response_01.text:
                        temp_1 = response_02.json()['Timestamp']
                        temp_2 = response_03.json()['Timestamp']

                        # A sequência de ifs verifica qual disjuntor abriu por último
                        if temp_1 > temp_2:
                            temp = temp_1
                        elif temp_1 < temp_2:
                            temp = temp_2
                        else:
                            temp = temp_1
                        
                        ## Criando Ocorrência
                        relat = Relat.setup('config.json')
                        ocorrencia = relat.models.ocorrenciaOPU(
                            TimestampOcorrencia=datetime.now(),
                            TipoOcorrencia_DescricaoPT="Teste RELAT PT",
                            # 438 caracteres/linha
                            DescricaoOcorrencia='Respondendo a solicitação do sistema, unidade em MANUAL no CAG/CAT e início da redução de carga. <br> <br>' + temp[11:21] + ' - Acionado Comando de Parada da Unidade. <br> <br>xx:xx - Unidade Parada. <br> <br><u>Manobras operacionais:</u><ul><li>PWB-' + equipamento['name'] + ': Selecionada bomba 02 como principal.</li><li>KH-xx: Desconectada central evaporativa.</li></li></ul>Despacho de Carga - xxxxx.'
                            )
                        ## Cadastrando Ocorrência
                        cadastro = relat.insert.cadastrarOcorrenciaOPU(ocorrencia)
                        print('Relatado comando de parada')
                        continue
                

                # Verifica se a seccionadora do vão está fechada
                if "FECHAD.AC" or "CERRAD" in response_01.text:
                    if equipamento['Tipo Ocorrencia'] == 'Comando de partida':
                        ## Criando Ocorrência
                        relat = Relat.setup('config.json')
                        ocorrencia = relat.models.ocorrenciaOPU(
                            TimestampOcorrencia=datetime.now(),
                            TipoOcorrencia_DescricaoPT="Teste RELAT PT",
                            # 438 caracteres/linha
                            DescricaoOcorrencia='<p>Atendimento a solicita&ccedil;&atilde;o do sistema, acionado comando de partida da unidade.<br /><u>Manobras preliminares:</u></p><ul><li>PWD-' + equipamento['name'] + 'Selecionada bomba 1 como principal.</li><li>KH-39: Conectada central evaporativa.</li></ul><p>xx:xx - Unidade com velocidade nominal em 100%.<br />xx:xx - Fechado disjuntor xxxxx, sincronizando a unidade com o sistema.<br />xx:xx - Unidade em carga.<br />xx:xx - Unidade em AUTO no CAG/CAT.</p><p>Despacho de carga - xxxxx</p><p><br />&nbsp;</p>'
                            )

                        ## Cadastrando Ocorrência
                        cadastro = relat.insert.cadastrarOcorrenciaOPU(ocorrencia)
                        print("Relatado comando de partida")
                break



def stop_loop(loop, rcv_signal):
    if rcv_signal == signal.SIGINT:
        rcv_signal = 'SIGINT'
    elif rcv_signal == signal.SIGTERM:
        rcv_signal = 'SIGTERM'
    log.info(f"Recieve {rcv_signal}: Stop process gracefully")
    loop.stop()


async def main():
    log.setup(
        path_to_save='.',
        project_name='Ocorrencias-Relat-Websocket',
        level="debug",
        log_to_console=True,
    )
    log.info("Starting program!")

    # Lê o json contendo os equipamentos (alarmes de parada)
    # Ler todos os webid's deste arquivo
    webids = read_webids(os.path.join('.', "estado_equipamentos.json"))
    log.info(f"Found {len(webids)*len(webids[0].split('webId'))} webids")

    # Log na rede pi
    user = ''
    password = ''
    auth = user + ':' + password
    authHeader = 'Basic ' + base64.b64encode(auth.encode('utf-8')).decode('utf-8')
    headers = {'Authorization': authHeader}
    tasks = []

    # Monitora a mudança de estado do ponto 
    # Percorre os webids, montando links
    for webid in webids:
        tasks.append(asyncio.create_task(websocket_client(
            url="wss://pi-api-vm01.siri.itaipu/piwebapi/streamsets/channel?" + webid,
            headers=headers,
            message_callback=message_callback
        ))
        )

    log.info(f"Spawning {len(tasks)} tasks")
    # add singal handler for linux
    if os.name == "posix":
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(
            signal.SIGINT, lambda: stop_loop(loop, signal.SIGINT))
        loop.add_signal_handler(
            signal.SIGTERM, lambda: stop_loop(loop, signal.SIGTERM))
    await asyncio.gather(*tasks)
    log.info("Program finished!")


if __name__ == "__main__":
    asyncio.run(main())

# %%
