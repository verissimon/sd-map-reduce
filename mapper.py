import json
from coordinator import MAPPER_COMPLETION_CHANNEL, MAPPER_QUEUE, REDIS_HOST, REDIS_PORT
import redis
import time
import logging
import os
import re

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MapReduce-Mapper')

logger.info(f"Conectando ao Redis em {REDIS_HOST}:{REDIS_PORT}")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def map_function(line):
    """
    A função map. Neste exemplo, é um contador de palavras.
    Recebe uma linha de texto e emite pares (word, 1) para cada palavra.
    """
    # Converte para lowercase e divide em palavras com regex
    words = re.findall(r'\w+', line.lower())
    
    result = {}
    for word in words:
        if word in result:
            result[word].append(1)
        else:
            result[word] = [1]
    
    return result

def process_chunk(input_file, output_file):
    """Processa um chunk do arquivo de entrada."""
    logger.info(f"Processing chunk: {input_file}")
    
    all_key_values = {}
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                kv_pairs = map_function(line)
                
                # Junta os resultados
                for key, values in kv_pairs.items():
                    if key not in all_key_values:
                        all_key_values[key] = []
                    all_key_values[key].extend(values)
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_key_values, f)
        
        logger.info(f"Saída de mapper escrita em {output_file} com {len(all_key_values)} keys únicas")
        return True
    
    except Exception as e:
        logger.error(f"Erro ao processar chunk {input_file}: {str(e)}")
        return False

def run_mapper():
    """
    Executa o mapper worker que recebe tasks da fila Redis e os processa.
    """
    logger.info("Mapper worker iniciou")
    
    while True:
        try:
            # Tenta obter uma tarefa da fila com um timeout
            # Isso permite que o worker verifique periodicamente se deve encerrar
            task_data = redis_client.blpop(MAPPER_QUEUE, timeout=1)
            
            if task_data:
                # task_data é uma tupla do tipo (queue_name, data)
                _, task_json = task_data
                
                task = json.loads(task_json)
                task_id = task['task_id']
                input_file = task['input_file']
                output_file = task['output_file']
                
                logger.info(f"Task recebida: {task_id} - Processando {input_file}")
                
                success = process_chunk(input_file, output_file)
                
                if success:
                    redis_client.publish(MAPPER_COMPLETION_CHANNEL, task_id)
                    logger.info(f"Task {task_id} completa com sucesso.")
                else:
                    logger.error(f"Task {task_id} falhou.")
            
            else:                
                logger.debug("Sem tasks disponíveis. esperando...")
        
        except Exception as e:
            logger.error(f"Erro no worker de map: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    run_mapper()
    