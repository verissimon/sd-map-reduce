import random
import os
import logging

def generate_random_paragraph(min_words=20, max_words=100):
    """Gera um parágrafo aleatório com um número aleatório de palavras."""
    num_words = random.randint(min_words, max_words)
    paragrafo = []
    
    palavras = [
        "o", "de", "a", "e", "que", "do", "da", "em", "um", "para",
        "é", "com", "não", "uma", "os", "no", "se", "na", "por", "mais",
        "as", "dos", "como", "mas", "foi", "ao", "ele", "das", "tem", "à",
        "seu", "sua", "ou", "ser", "quando", "muito", "há", "nos", "já", "está",
        "eu", "também", "só", "pelo", "pela", "até", "isso", "ela", "entre", "depois",
        "sem", "mesmo", "aos", "seus", "quem", "nas", "me", "esse", "eles", "estão",
        "você", "tinha", "foram", "essa", "num", "nem", "suas", "meu", "às", "minha",
        "têm", "numa", "pelos", "elas", "havia", "seja", "qual", "será", "nós", "tenho",
        "lhe", "deles", "essas", "esses", "pelas", "este", "dele", "tu", "te", "vocês",
        "vos", "lhes", "meus", "minhas", "teu", "tua", "teus", "tuas", "nosso", "nossa",
        "nossos", "nossas", "dela", "delas", "esta", "estes", "estas", "aquele", "aquela", "aqueles",
        "aquelas", "isto", "aquilo"
    ]
    
    for _ in range(num_words):
        paragrafo.append(random.choice(palavras))
        
    
    # Inserindo pontuações aleatórias
    for i in range(2, len(paragrafo), random.randint(3, 8)):
        if i < len(paragrafo):
            paragrafo[i] = paragrafo[i] + random.choice(['.', ',', ';', ':', '?', '!'])
    
    # Capitalizando palavras
    paragrafo[0] = paragrafo[0].capitalize()
    for i in range(1, len(paragrafo)):
        if i > 0 and paragrafo[i-1].endswith('.'):
            paragrafo[i] = paragrafo[i].capitalize()
    
    return ' '.join(paragrafo)

def generate_test_data(file_path, size_mb=10):
    """Gera um arquivo de dados de teste com aproximadamente o tamanho especificado."""
    tamanho_total = size_mb * 1024 * 1024
    tamanho_atual = 0
    
    with open(file_path, 'w', encoding='utf-8') as f:
        while tamanho_atual < tamanho_total:
            paragrafo = generate_random_paragraph() + '\n'
            tamanho_paragrafo = len(paragrafo.encode('utf-8'))
            
            f.write(paragrafo)
            tamanho_atual += tamanho_paragrafo
            
            # Logando progresso
            if tamanho_atual % (10 * 1024 * 1024) < tamanho_paragrafo:
                logging.info(f"Gerou {tamanho_atual / (1024 * 1024):.2f} MB")
    
    tamanho_final = os.path.getsize(file_path)
    logging.info(f"Arquivo de teste gerado: {file_path} ({tamanho_final / (1024 * 1024):.2f} MB)")

if __name__ == "__main__":
    generate_test_data("./data/data.txt")