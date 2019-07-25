import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
#import oauth, tweepy
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import time

def main():
    foo = andamentos(1,'ALESSANDRO MOLON')
    foo = andamentos(1,'PARTIDO SOCIALISTA BRASILEIRO')
    foo = send_to_sheets()
   # foo.main()

def get_processos(pagina, parte):

    tabelas = request(pagina, parte)
    links = []
    data = []
    processos = []

    for tabela in tabelas:
        for a in tabela.find_all('a', href=True):
            links.append("https://portal.stf.jus.br/processos/%s"%a['href'])


        rows = tabela.find_all('tr')


        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele]) # Get rid of empty values

    return(links,data)

def request(pagina, parte):

    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}

    page = requests.get('https://portal.stf.jus.br/processos/listarPartes.asp?termo='+parte+'&paginaAtual='+str(pagina),headers=headers, timeout=10)

    try:

        if page.status_code != 200:
            return False
        else:

            soup = BeautifulSoup(page.content, 'html.parser')
            tabela = soup.find('table')
            tabelas = []
            n_acoes = 0

            while tabela != None:
                page = requests.get('https://portal.stf.jus.br/processos/listarPartes.asp?termo='+parte+'&paginaAtual='+str(pagina),headers=headers, timeout=10)
                soup = BeautifulSoup(page.content, 'html.parser')
                tabela = soup.find('table')

                if tabela == None:
                    break
                else:
                    tabelas.append(tabela)
                    pagina+=1
                    n_acoes = n_acoes+(len(tabela.find_all('tr')))

            print('foram encontradas %s ações'%n_acoes)

            return(tabelas)

    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))
    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
    except KeyboardInterrupt:
        print("Someone closed the program")

def andamentos(pagina,parte):

    urls_processos, data = get_processos(pagina,parte)
    andamentos = []
    data_movimentacao = []
    relatores = []
    #detalhes=[]

    headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}

    try:

        for i in range(0, len(urls_processos)):
            digits = [d for d in urls_processos[i] if d.isdigit()]
            digit = ''.join(digits)

            page = requests.get('https://portal.stf.jus.br/processos/abaAndamentos.asp?incidente='+str(digit)+'&imprimir=',headers=headers, timeout=50)

            if page.status_code != 200:
                return False
            else:

                soup = BeautifulSoup(page.content, 'html.parser')

                andamento = soup.find("h5", {"class": "andamento-nome "}).get_text()
                detalhe = soup.find("div", {"class": "col-md-9 p-0"}).get_text()
                datas = soup.find("div", {"class": "andamento-data "}).get_text()
                t = andamento + " " + detalhe
                andamentos.append(t)
                data_movimentacao.append(datas)
                #detalhes.append(detalhe)

    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))
    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
    except KeyboardInterrupt:
        print("Someone closed the program")

    try:

        for j in range(0, len(urls_processos)):
            digits = [d for d in urls_processos[j] if d.isdigit()]
            digit = ''.join(digits)

            page = requests.get('https://portal.stf.jus.br/processos/detalhe.asp?incidente='+str(digit),headers=headers, timeout=50)

            if page.status_code != 200:
                return False
            else:
                soup = BeautifulSoup(page.content, 'html.parser')

                cabeçalho = soup.findAll("div", {"class": "processo-dados p-l-16"})

                if cabeçalho:

                    relator_atual = cabeçalho[1].get_text().split(":")[1]
                    relatores.append(relator_atual)
                else:
                    print('html page has changed. please, fix it.')

    except requests.ConnectionError as e:
        print("OOPS!! Connection Error. Make sure you are connected to Internet. Technical Details given below.\n")
        print(str(e))
    except requests.Timeout as e:
        print("OOPS!! Timeout Error")
        print(str(e))
    except requests.RequestException as e:
        print("OOPS!! General Error")
        print(str(e))
    except KeyboardInterrupt:
        print("Someone closed the program")

    data = to_pandas(data,urls_processos, andamentos, data_movimentacao,relatores,parte)

    return (data)

def to_pandas(data, urls_processos, andamentos, data_movimentacao,relatores,parte):
   # pd.Series([3,4])
    df = pd.DataFrame(np.array(data).reshape(len(data),7), columns = ['processo','parte','numero_único','data_autuação','meio','publicidade','trâmite'])
    df['link'] = pd.Series(urls_processos)
    df['andamento'] = pd.Series(andamentos)
    df['data_ultima_movimentação'] = pd.Series(data_movimentacao)
    df['relator_atual'] = pd.Series(relatores)
    #df['detalhe'] = pd.Series(detalhes)

    df.to_csv('your path here!')

    return df

def send_to_sheets():

    partes = ['ALESSANDRO MOLON','PARTIDO SOCIALISTA BRASILEIRO']

    for parte in partes:

        df = pd.read_csv('your path here!', encoding = 'utf-8')

        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name('your credentials here!', scope)
        client = gspread.authorize(creds)
        ws = client.open('stf_teste').worksheet(parte)
        gd.set_with_dataframe(ws, df, row=1, col=1, resize=True)

        print ('sent %s to sheets'%parte)

if __name__ == '__main__':
	main()
