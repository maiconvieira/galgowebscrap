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

# Importar função criada retrocederData
from retrocederData import *

# checar se diretório existe se não existir criar
parent_dir = 'arquivos'

# carregar a variavel racingDate com a data de ontem
date_now = datetime.datetime.now()
racingDate = date_now.strftime('%Y-%m-%d')

# Link com variavel racingpost para rastrear
driver.get('https://www.timeform.com/greyhound-racing/results/yesterday')
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
driver.quit()