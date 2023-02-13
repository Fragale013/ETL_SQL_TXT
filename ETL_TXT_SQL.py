#Primeiramente vamos importar as bibliotecas
#Pyodbc irá realizar a conexão com o sgdb, urllib realizará o download do TXT, pandas manipulará os dados, tqdm para mostrar visualmente o avanço da ETL

import pyodbc
from urllib import request
from datetime import datetime
import pandas as pd
from tqdm import tqdm
from time import sleep

#Vamos inicialmente criar uma variável com os dados de conexão
#No meu caso não é necessária autenticação com usuário e senha, pois é um servidor SQL Server local, sugiro ler a documentação oficial da pyodbc para escalar conforme necessário
dados_conexao = (
    "Driver={SQL Server};"
    "Server=ANALISTA-143A;"
    "Database=BASES"
)
conexao = pyodbc.connect(dados_conexao)
print('Conexão criada com sucesso.')
cursor = conexao.cursor()

##################################################
#Depois de criar a conexão, no meu caso preciso limpar a tabela temporária, para isso criei uma função que utiliza SQL para realizar o delete de quaisquer dados que possam estar na tabela
def limpar_base():
    print('Iniciando limpeza da base')
    cursor.execute("Delete from BASE")
    cursor.commit()
    print('Base limpa e pronta para receber os novos dados')

#Agora preciso realizar o download do arquivo txt, para isso preciso saber o link e o destino onde ele ficará salvo:
def download_base_txt():
    print('Iniciando download da base TXT')
    at5 = 'http://10.13.51.211/qEMP/db/base.txt'
    destino = 'C:/Users/user1/Downloads/BASES/base.txt'
    request.urlretrieve(at5, destino)
    print('Download da base TXT realizado com sucesso')
    
#Após o download, vamos percorrer o arquivo para de fato realizar o insert dos dados no banco, isso ocorrerá linha por linha
#Perceba que utilizamos o método .fillna do pandas para tratar algumas colunas que possam conter dados nulos, de forma a preencher com 0 os valores inválidos
#Perceba também que o pandas precisa que o python "force" alguns tipos de dados para STRING ou INT, de forma que o SQL Server aceite os dados conforme o datatype definido do sgbd
#Utilizamos as variáveis "?,?,?,?" para ocultar os dados na transação, isso impede o ato malicioso de SQL Injection nos dados
#Após ele percorrer todas as linhas, ele irá realizar o commit da transação inserindo de fato os dados na tabela do banco
#O TQDM irá exibir uma linha no prompt com o avanço do processo
def inserir_linhas_db():
    print('Iniciando o input de dados no banco')
    headers = ["COD_OPERADORA",	"SIGLA", "ID_REGIAO", "STATUS",	"DT_NOTA", "COD_OS", "NUM_CONTRATO", "NOME_TITULAR", "FECHAMENTO", "SEGMENTO", "TIPO_ORD_SRV", "DT_INST_ASS",	"DT_ATEND",	"DT_CADASTRO", "DT_BAIXA", "COD_BAIXA",	"COD_CANCEL",	"ULT_REAGENDAMENTO_GERAL", "ULT_USR_REAGENDAMENTO_GERAL", "COD_BAIXA_1", "COD_BAIXA_2",	"COD_BAIXA_3", "COD_BAIXA_4",	"COD_BAIXA_5", "LOG_REAGENDA", "LOG_VT", "COD_OS_1", "COD_OS_2", "COD_OS_3", "COD_OS_4", "COD_OS_5", "ULT_COD_BAIXA",	"ULT_COD_OS", "AREA_DESPACHO", "COD_NODE", "COD_IMOVEL", "END_COMPLETO", "TIPO_COMPLEMENTO", "NR_PROTOCOLO_BP",	"DDD_TELEFONE_VOIP", "NUM_TELEFONE_VOIP",	"DT_DESPACHO", "DESP_PARCEIRA",	"DESP_EQUIPE", "EXEC_PARCEIRA",	"EXEC_EQUIPE", "HR_INICIO_EXECUCAO", "HR_TERMINO_EXECUCAO",	"DT_AGENDA",	"AGENDA_DESCR",	"CONVENIENCIA_AUTO", "EMERGENCIA", "IMEDIATA", "ISENTO_COBRANCA",	"NO_OCORRENCIA_IE",	"ID_PONTO",	"PRODUTO_TIPO_ANTIGO", "PRODUTO_TECNOLOG_ANTIGO",	"PRODUTO_ANTIGO",	"PRODUTO_TIPO_ATUAL",	"PRODUTO_TECNOLOG_ATUAL",	"PRODUTO_ATUAL",	"PRODUTO_TIPO_NOVO",	"PRODUTO_TECNOLOG_NOVO", "PRODUTO_NOVO",	"USR_ATEND",	"USR_DESPACHO",	"USR_BAIXA", "USR_ATEND_PF", "USR_DESPACHO_PF",	"USR_BAIXA_PF",	"IE_DESCRICAO",	"IE_DT_OCORRENCIA",	"IE_DT_RESOLUCAO",	"IE_TP_RESOLUCAO",	"IE_USR_ATEND",	"IE_USR_RESOL",	"OBS"]
    with tqdm(total=100) as barra_progresso:
        table = pd.read_table('Users/user1/Downloads/BASES/base.txt', sep=';',on_bad_lines='skip', low_memory=False, skiprows=[1,1])
        table['DT_AGENDA'].fillna(value=0, inplace=True)
        table.fillna(value="", inplace=True)
        
        for index, linha in table.iterrows():
          try:
            cursor.execute("Insert into AT5 (COD_OPERADORA, ID_REGIAO, STATUS, DT_NOTA, COD_OS, NUM_CONTRATO, NOME_TITULAR, FECHAMENTO, SEGMENTO, TIPO_ORD_SRV, DT_INST_ASS, DT_ATEND, DT_CADASTRO, DT_BAIXA, COD_BAIXA, COD_CANCEL, LOG_VT, AREA_DESPACHO, COD_NODE, COD_IMOVEL, END_COMPLETO, TIPO_COMPLEMENTO, NR_PROTOCOLO_BP, DDD_TELEFONE_VOIP, NUM_TELEFONE_VOIP, DT_DESPACHO, DESP_PARCEIRA, DESP_EQUIPE, EXEC_PARCEIRA, EXEC_EQUIPE, HR_INICIO_EXECUCAO, HR_TERMINO_EXECUCAO, DT_AGENDA, AGENDA_DESCR, CONVENIENCIA_AUTO, EMERGENCIA, IMEDIATA, ISENTO_COBRANCA, NO_OCORRENCIA_IE, ID_PONTO, PRODUTO_TIPO_ANTIGO, PRODUTO_TECNOLOG_ANTIGO, PRODUTO_ANTIGO, PRODUTO_TIPO_ATUAL, PRODUTO_TECNOLOG_ATUAL, PRODUTO_ATUAL, PRODUTO_TIPO_NOVO, PRODUTO_TECNOLOG_NOVO, PRODUTO_NOVO, USR_ATEND, USR_DESPACHO, USR_BAIXA,USR_ATEND_PF, USR_DESPACHO_PF, USR_BAIXA_PF, IE_DESCRICAO, IE_DT_OCORRENCIA, IE_DT_RESOLUCAO, IE_TP_RESOLUCAO, IE_USR_ATEND, IE_USR_RESOL)values( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  ?, ?, ?, ?, ?, ?, ?, ?, ?,	?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",linha.COD_OPERADORA, str(linha.ID_REGIAO), str(linha.STATUS), linha.DT_NOTA, str(linha.COD_OS), linha.NUM_CONTRATO, str(linha.NOME_TITULAR), str(linha.FECHAMENTO), str(linha.SEGMENTO), str(linha.TIPO_ORD_SRV), linha.DT_INST_ASS, linha.DT_ATEND, linha.DT_CADASTRO, linha.DT_BAIXA, linha.COD_BAIXA, linha.COD_CANCEL, linha.LOG_VT, str(linha.AREA_DESPACHO), str(linha.COD_NODE), linha.COD_IMOVEL, str(linha.END_COMPLETO), str(linha.TIPO_COMPLEMENTO), str(linha. NR_PROTOCOLO_BP), str(linha. DDD_TELEFONE_VOIP), str(linha.NUM_TELEFONE_VOIP), linha.DT_DESPACHO, str(linha.DESP_PARCEIRA), str(linha.DESP_EQUIPE), str(linha.EXEC_PARCEIRA), str(linha.EXEC_EQUIPE), linha.HR_INICIO_EXECUCAO, linha.HR_TERMINO_EXECUCAO, linha. DT_AGENDA, str(linha.AGENDA_DESCR), str(linha.CONVENIENCIA_AUTO), str(linha.EMERGENCIA), str(linha.IMEDIATA), str(linha.ISENTO_COBRANCA), linha.NO_OCORRENCIA_IE, linha.ID_PONTO, str(linha.PRODUTO_TIPO_ANTIGO), str(linha.PRODUTO_TECNOLOG_ANTIGO), str(linha.PRODUTO_ANTIGO), str(linha.PRODUTO_TIPO_ATUAL), str(linha.PRODUTO_TECNOLOG_ATUAL), str(linha.PRODUTO_ATUAL), str(linha.PRODUTO_TIPO_NOVO), str(linha.PRODUTO_TECNOLOG_NOVO), str(linha.PRODUTO_NOVO), str(linha.USR_ATEND), str(linha.USR_DESPACHO), str(linha.USR_BAIXA), str(linha.USR_ATEND_PF), str(linha.USR_DESPACHO_PF), str(linha.USR_BAIXA_PF), str(linha.IE_DESCRICAO), linha.IE_DT_OCORRENCIA, str(linha.IE_DT_RESOLUCAO), str(linha.IE_TP_RESOLUCAO), str(linha.IE_USR_ATEND), str(linha.IE_USR_RESOL))
          except Exception as erro:
             print(erro)
             pass
          barra_progresso.update(100/len(at5_table.index))
    cursor.commit()
    print("Dados incluídos com sucesso")
    
##################################################
#Por fim, vamos chamar as funções para executar a ETL:
#O código irá printar no prompt o horário que está iniciando, irá mostrar eventuais erros no processo de insert das linhas, mostrará o avanço do processo em uma barrinha de progresso e por fim vai mostrar o horário de conclusão, aguardar uns segundos e encerrar o prompt

data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')
print(f'Iniciando rotina ETL TXT, {data_e_hora_em_texto}')
print(' ')
download_base_txt()
limpar_base_txt()
inserir_linhas_db()
data_e_hora_atuais = datetime.now()
data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')
print(' ')
print(f'Encerrando rotina ETL TXT, {data_e_hora_em_texto}')
sleep()



