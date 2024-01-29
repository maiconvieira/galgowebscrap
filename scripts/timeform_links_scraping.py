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

# checar se diretório existe se não existir cirar
parent_dir = "arquivos"

if os.path.exists(parent_dir) != True:
    os.mkdir(parent_dir)

# carregar a variavel racingDate com o dia de hoje
date_now = datetime.datetime.now()
two_days_ago = date_now - datetime.timedelta(days=2)
racingDate = two_days_ago.strftime("%Y-%m-%d")

# arquivo para salvar os links
nome_arquivo = "UltimoDiaScaneado.txt"
path_file = os.path.join(parent_dir, nome_arquivo)

# se o arquivo existe ok, se não existir criar
try:
    arquivo = open(path_file, 'r+')
    if os.stat(path_file).st_size == 0:
        pass
    else:
        with open(path_file) as file:
            for line in file:
                pass
            partsOfDate = line.split('-')
            lastLineYear = int(partsOfDate[0])
            lastLineMonth = int(partsOfDate[1])
            lastLineDay = int(partsOfDate[2])
            if lastLineDay > 1 and lastLineMonth > 1:
                dayToScrap = str(lastLineDay - 1).zfill(2)
                monthToScrap = str(lastLineMonth).zfill(2)
                yearToScrap = str(lastLineYear).zfill(4)
            elif lastLineDay > 1 and lastLineMonth == 1:
                dayToScrap = str(lastLineDay - 1).zfill(2)
                monthToScrap = str(lastLineMonth).zfill(2)
                yearToScrap = str(lastLineYear).zfill(4)
            elif lastLineDay == 1 and lastLineMonth == 3 and ((lastLineYear%4==0 and lastLineYear%100!=0) or (lastLineYear%400==0)):
                dayToScrap = str(29)
                monthToScrap = str(lastLineMonth - 1).zfill(2)
                yearToScrap = str(lastLineYear).zfill(4)
            elif lastLineDay == 1 and lastLineMonth in(2, 4, 6, 8, 9, 11):
                dayToScrap = str(31)
                monthToScrap = str(lastLineMonth - 1).zfill(2)
                yearToScrap = str(lastLineYear).zfill(4)
            elif lastLineDay == 1 and lastLineMonth in(5, 7, 10, 12):
                dayToScrap = str(30)
                monthToScrap = str(lastLineMonth - 1).zfill(2)
                yearToScrap = str(lastLineYear).zfill(4)
            elif lastLineDay == 1 and lastLineMonth == 1:
                dayToScrap = str(31)
                monthToScrap = str(12)
                yearToScrap = str(lastLineYear -1).zfill(4)
            racingDate = yearToScrap + '-' + monthToScrap + '-' + dayToScrap
except FileNotFoundError:
    arquivo = open(path_file, 'w+')

fileDayScraped = os.path.join(parent_dir, racingDate + '.txt')
f = open(fileDayScraped, 'w+')

# Link para rastrear
timeform_link = "https://www.timeform.com/greyhound-racing/results/"

# Link para rastrear
driver.get(timeform_link + racingDate)
driver.implicitly_wait(0.5)

fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")

for index, i in enumerate(fullpage):
    qty = index + 1
    hrefCaptured = i.get_attribute("href")
    if index != len(fullpage) - 1:
        f.write(hrefCaptured + '\n')
    else:
        f.write(hrefCaptured)
f.close()

print(str(qty) + " corrida(s) nesse dia.")
    
arquivo.write(racingDate)
arquivo.close()
driver.quit()