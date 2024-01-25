from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

timeform_link = "https://www.timeform.com/greyhound-racing/results/2024-01-15"

driver.get(timeform_link)
driver.implicitly_wait(0.5)

fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
qty = 0

for i in fullpage:
    qty = qty + 1
    print(i.get_attribute("href"))

print("")
print(str(qty) + " corrida(s) nesse dia.")
driver.quit()