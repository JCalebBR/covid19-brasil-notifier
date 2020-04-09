import json
from urllib.parse import urlencode

import certifi
import urllib3

default_header = {
    "x-parse-application-id": "unAFkcaNDeXajurGB7LChj8SgQYS2ptm"
}

def lambda_handler(event, context):
    try:
        if event["rawQueryString"] == "scrape=true":
            try:
                http = urllib3.PoolManager(ca_certs=certifi.where())
                r = http.request('GET', 
                            'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral',
                            headers=default_header)
                if total.status == 200:
                    total = json.loads(r.data.decode('utf-8'))
                    
                    info = {
                        "updatedAt" : total['results'][0]['dt_atualizacao']
                    }
                    
                    try:
                        with open('/tmp/update.json', 'r') as f:
                            data = json.loads(f.read())
                        if data["updatedAt"] == r['results'][0]['dt_atualizacao']:
                            return "Latest data is available"
                    except FileNotFoundError:
                        with open('/tmp/update.json', 'w') as f:
                            f.write(json.dumps(info, indent=4))
                else:
                    return "Malformed 'total' data"
                
                payload = f"""
*Contagem nacional:*
    *Casos confirmados:* {total['results'][0]['total_confirmado']}
    *Óbitos:* {total['results'][0]['total_obitos']}
    *Taxa de letalidade:* {total['results'][0]['total_letalidade']}\n
Divisão por estados:"""

                r = http.request('GET', 
                            'https://xx9p7hp1p7.execute-api.us-east-1.amazonaws.com/prod/PortalGeral',
                            headers=default_header)
                
                if estados.status == 200:
                    _estados = json.loads(r.data.decode('utf-8'))
                    estados = {}
                    for estado in estados['results']:
                        estados.update({
                            estado['nome']: estado['qtd_confirmado']
                        })
                    for estado, quantidade in estados.items():
                        payload += '\n\t' + estado + ': ' + str(quantidade)
                else:
                    return "Malformed 'estados' data"

                payload += f"""
Atualizado em {total['results'][0]['dt_atualizacao']}
            _via https://covid.saude.gov.br_"""

                TOKEN = "TOKEN"
                chat_id = "CHATID"
                bot_url = "https://api.telegram.org/bot" + TOKEN + "/sendMessage?"

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
    lambda_handler({'scrape': 'true'},'')
