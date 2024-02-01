# Instalar e configurar selenium e chrome driver.

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless')
#options.add_argument('--no-sandbox')
options.add_argument('log-level=3') # INFO = 0 / WARNING = 1 / LOG_ERROR = 2 / LOG_FATAL = 3
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Import bibliotecas
import os
import datetime
import sys

# Importar função criada retrocederData
from retrocederData import *

# checar se diretório existe se não existir criar
parent_dir = 'arquivos'

if os.path.exists(parent_dir) != True:
    os.mkdir(parent_dir)

# arquivo para salvar os links
path_file = os.path.join(parent_dir, 'UltimoDiaEscaneado.txt')

# carregar a variavel racingDate com a data de ontem
date_now = datetime.datetime.now()
two_days_ago = date_now - datetime.timedelta(days=1)
racingDate = two_days_ago.strftime('%Y-%m-%d')

# se o arquivo existe ok, se não existir criar
if os.path.isfile(path_file):
    if os.stat(path_file).st_size != 0:
        with open(path_file) as f:
            line = f.readline()
            # Se a variavel line for igual a '2013-01-01' abortar o script.
            if line == '2013-01-01':
                f.close()
                os.remove(path_file)
                sys.exit()
            racingDate = retrocederData(line)
        f.close()

# Parte do link para rastrear
timeform_link = 'https://www.timeform.com/greyhound-racing/results/'

# Link com variavel racingpost para rastrear
driver.get(timeform_link + racingDate)
#driver.get(timeform_link + '2023-12-26')
driver.implicitly_wait(0.5)
fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")

# Arquivo onde será salvo os links do dia escaneado.
pathScrapedDay = os.path.join(parent_dir, racingDate + '.txt')

if len(fullpage) != 0:
    # Checando se a lista esta vazia ou não.
    if not os.path.isfile(pathScrapedDay):
        with open(pathScrapedDay, 'w+') as f:
            for index, i in enumerate(fullpage):
                qty = index + 1
                hrefCaptured = i.get_attribute("href")
                if index != len(fullpage) - 1:
                    f.write(hrefCaptured + '\n')
                else:
                    f.write(hrefCaptured)
        f.close()
else:
    racingDate = retrocederData(racingDate)

# Atualizando data do arquivo 'UltimoDiaEscaneado.txt'
with open(path_file, 'w+') as f:
    f.write(racingDate)
f.close()
driver.quit()