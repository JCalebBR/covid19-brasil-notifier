import json
from urllib.parse import urlencode

import certifi
import urllib3

import uploader

default_header = {
    "x-parse-application-id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm"
}
update_file = 'tmp/update2.json'
object_name = update_file.replace('tmp/','')
bucket = 'corona-scraper-bucket'

def lambda_handler(event, context):
    try:
        if event["rawQueryString"] == "scrape=true":
            try:
                http = urllib3.PoolManager(ca_certs=certifi.where())
                r = http.request('GET', 
                            'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral',
                            headers=default_header)
                if r.status == 200:
                    total = json.loads(r.data.decode('utf-8'))
                    
                    info = {
                        "updatedAt" : f"{total['results'][0]['dt_atualizacao']}"
                    }
                    
                    try:
                        if uploader.download_file(update_file, bucket, object_name):
                            with open(update_file, 'r') as f:
                                data = json.loads(f.read())
                            if data["updatedAt"] == total['results'][0]['dt_atualizacao']:
                                raise FileExistsError
                            else:
                                raise FileNotFoundError
                        else: 
                            raise FileNotFoundError
                    except FileNotFoundError:
                        uploader.upload_file(info, update_file, bucket, object_name)
                        pass
                    except FileExistsError:
                        new_data = "Oi you there, stop!"
                else:
                    return "Malformed 'total' data"
                
                payload = f"""
*Contagem nacional:*
    *Casos confirmados:* {total['results'][0]['total_confirmado']}
    *Óbitos:* {total['results'][0]['total_obitos']}
    *Taxa de letalidade:* {total['results'][0]['total_letalidade']}\n
*Divisão de casos confirmados por estado:*"""

                r = http.request('GET', 
                            'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalMapa',
                            headers=default_header)
                
                if r.status == 200:
                    _estados = json.loads(r.data.decode('utf-8'))
                    estados = {}
                    for estado in _estados['results']:
                        estados.update({
                            f"*{estado['nome']}*": estado['qtd_confirmado']
                        })
                    del _estados
                    for estado, quantidade in estados.items():
                        payload += '\n\t' + estado + ': ' + str(quantidade)
                else:
                    return print("Malformed 'estados' data")

                payload += f"""\n\n\
Atualizado em {total['results'][0]['dt_atualizacao']}
            via [Coronavírus Brasil](https://covid.saude.gov.br)"""

                TOKEN = "866395170:AAGIPgNThi3SvSqAOlHjroPV2bAQyrBSL3I"
                chat_id = "@brasilcovid19"
                bot_url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?"

                try:
                    data = urlencode({
                        "chat_id":chat_id,
                        "text":new_data,
                        "parse_mode":"Markdown"
                    })
                except NameError:
                    data = urlencode({
                        "chat_id":chat_id,
                        "text":payload,
                        "parse_mode":"Markdown"
                    })
                
                bot_url = bot_url + data
                t = http.request('POST', bot_url)
                if t.status == 200:
                    return "Ok"
                else:
                    return f"Error on telegram request {t.status}"
            except Exception as ex:
                return f"Error on covid request {ex}"
        else:
            return {
                "error" : "no scraping done"
            }
    except KeyError:
        return {
            "error" : "malformed request"
        }

if __name__ == "__main__":
    lambda_handler({'rawQueryString': 'scrape=true'},'')
