import os
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def mesclar_arquivos_recuperados(diretorio_data="./data"):
    arquivos = os.listdir(diretorio_data)
    
    arquivos_primarios = [f for f in arquivos if "_scraped_1_" in f]
    
    for arq_1 in arquivos_primarios:
        arq_2 = arq_1.replace("_scraped_1_", "_scraped_2_")
        arq_3 = arq_1.replace("_scraped_1_", "_scraped_3_")
        
        caminho_1 = os.path.join(diretorio_data, arq_1)
        caminho_2 = os.path.join(diretorio_data, arq_2)
        caminho_3 = os.path.join(diretorio_data, arq_3)
        
        if not os.path.exists(caminho_2):
            continue
            
        with open(caminho_1, 'r', encoding='utf-8') as f1:
            dados_1 = json.load(f1)
            
        with open(caminho_2, 'r', encoding='utf-8') as f2:
            dados_2 = json.load(f2)
            
        chave_url = 'href_tf' if '_tf.json' in arq_1 else 'href_gh'
        
        corridas_unicas = {}
        for corrida in dados_1 + dados_2:
            url = corrida.get(chave_url)
            if url:
                corridas_unicas[url] = corrida
                
        lista_mesclada = list(corridas_unicas.values())
        
        lista_mesclada.sort(
            key=lambda x: (
                str(x.get('pista', '')),
                str(x.get('horario', ''))
            )
        )
        
        os.replace(caminho_1, caminho_3)
        
        with open(caminho_1, 'w', encoding='utf-8') as f1_novo:
            json.dump(lista_mesclada, f1_novo, indent=4, ensure_ascii=False)
            
        logging.info(f"Original movido para {arq_3}. {arq_1} atualizado com {len(lista_mesclada)} registros (Base: {len(dados_1)}, Recuperados: {len(dados_2)}).")

if __name__ == '__main__':
    mesclar_arquivos_recuperados()