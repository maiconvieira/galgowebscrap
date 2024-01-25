from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

driver.implicitly_wait(0.5)
# ...
timeform_link = "https://www.timeform.com/greyhound-racing/results/2024-01-15"
# ...
driver.get(timeform_link)
fullpage = driver.find_elements(By.XPATH, "//a[@class='waf-header hover-opacity']")
qty = 0

for i in fullpage:
    qty = qty + 1
    print(i.get_attribute("href"))

print("")
print(str(qty) + " corrida(s) nesse dia.")
driver.quit()