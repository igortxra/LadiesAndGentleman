FROM python:3.13-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o main.py para o container
COPY main.py .

# Instala dependências se precisar (mantido vazio por padrão)
 COPY requirements.txt .
 RUN pip install --no-cache-dir -r requirements.txt

# Comando para rodar o script
CMD ["python", "main.py"]
