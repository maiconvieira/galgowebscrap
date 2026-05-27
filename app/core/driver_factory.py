import os
import time
import shutil
import random
import logging
import threading
import undetected_chromedriver as uc
from selenium import webdriver

from selenium.webdriver.chrome.service import Service

from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from urllib3.exceptions import ProtocolError, MaxRetryError

driver_creation_lock = threading.Lock()

def configurar_driver_uc(driver_path: str):
    thread_id = threading.get_ident()
    driver_dir = os.path.dirname(driver_path)
    driver_name = os.path.basename(driver_path)
    new_driver_name = f"{driver_name}_{thread_id}"
    new_driver_path = os.path.join(driver_dir, new_driver_name)

    with driver_creation_lock:
        try:
            if not os.path.exists(new_driver_path):
                shutil.copy2(driver_path, new_driver_path)
        except Exception as e:
            logging.critical(f"Erro ao copiar driver para thread {thread_id}: {e}")
            return None   
     
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        options.add_argument(f'--user-agent={USER_AGENT}')
        
        try:
            driver = uc.Chrome(options=options, 
                               use_subprocess=True,
                               driver_executable_path=driver_path)

            driver.custom_exe_path = new_driver_path

            cooldown = random.uniform(5, 10)
            time.sleep(cooldown)
            return driver
        except Exception as e:
            logging.critical("Falha CRÍTICA ao criar a instância do driver (Undetected).", exc_info=True)
            logging.error("Liberando lock após falha na criação do driver.")
            return None

def configurar_driver_padrao(driver_path: str):
    with driver_creation_lock:
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        options.add_argument(f'--user-agent={USER_AGENT}')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        try:
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=options)
            cooldown = random.uniform(5, 10)
            time.sleep(cooldown)
            return driver
        except Exception as e:
            logging.critical("Falha CRÍTICA ao criar a instância do driver (Selenium Padrão).", exc_info=True)
            logging.error("Liberando lock após falha na criação do driver.")
            return None

def warm_up_driver(driver_instance, url_base: str):
    if not driver_instance:
        return None
    try:
        logging.info(f"Aquecendo o driver com a URL base: {url_base}")
        driver_instance.get(url_base)
        time.sleep(random.uniform(4, 7))
        if "timeform" in url_base:
            try:
                cookie_wait = WebDriverWait(driver_instance, 5)
                accept_button = cookie_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))
                accept_button.click()
                time.sleep(1)
            except TimeoutException:
                logging.info("Banner de cookies (Timeform) não encontrado durante o aquecimento. Seguindo.")
            except Exception as e_cookie:
                logging.warning(f"Erro ao tentar aceitar cookies no aquecimento: {e_cookie}")

        return driver_instance
    except (WebDriverException, MaxRetryError, ProtocolError) as e_warmup:
        logging.critical(f"FALHA NO AQUECIMENTO do driver ({type(e_warmup).__name__}). Tentando reiniciar...", exc_info=True)
        try:
            driver_instance.quit()
        except:
            pass
        return None