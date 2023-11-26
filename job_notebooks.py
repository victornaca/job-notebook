import ploomber_engine as pe
import multiprocessing
import shutil
import os
import pandas as pd
from datetime import datetime

# JSON de Configurações para rodar o ploomber e multiprocessing com caminho e notebook
configs = {
    "nome_config_1": {
    "notebook":"notebook1.ipynb",
    "filepath":"C:/Users/victor.santos/Notebooks/"
    },
    "nome_config_2": {
    "notebook":"notebook2.ipynb",
    "filepath":"C:/Users/victor.santos/Notebooks/"
    },
    "nome_config_3": {
    "notebook":"notebook3.ipynb",
    "filepath":"C:/Users/victor.santos/Notebooks/"  
    },
    "nome_config_4": {
    "notebook":"notebook4.ipynb",
    "filepath":"C:/Users/victor.santos/Notebooks/"
    },
    "nome_config_5": {
    "notebook":"notebook5.ipynb",
    "filepath":"C:/Users/victor.santos/Notebooks/"
    }
}

# Localização do Input e Output dos Dados (Usamos Excel/CSV)
input_path = os.path.join("C:/Users/victor.santos/Notebooks/", "input")
output_path = os.path.join("C:/Users/victor.santos/Notebooks/", "output")

# Files necessarios para rodar os notebooks
required_files = ["Arq1.xlsx", "Arq2.xlsx", "Arq3.xlsx"]

# Função para rodar o ploomber_engine e chamar os notebooks
def run_notebook(config):
    log_header = ["Notebook", "DateTimeStart", "DateTimeEnd", "StatusJob"]

    try:
        notebook = config['config']['notebook']
        filepath = config['config']['filepath']
        input = filepath + notebook

        print("-" * 50)
        print(config)
        print("-" * 50)

        start_time = datetime.now()
        status = "Success"

    # Função para executar os notebooks
        try:
            pe.execute_notebook(
            input,
            output_path=None,
            log_output=True,
            remove_tagged_cells="remove"
            )

        except Exception as e:
            print(f"Erro ao executar notebook {config['config']['notebook']}: {str(e)}")
            status = "Failed"

        end_time = datetime.now()
        # Log como lista
        log = [notebook, start_time, end_time, status]

        # Adiciona o log ao DataFrame do Pandas
        log_df = pd.DataFrame([log], columns=log_header)

        # Escreve o DataFrame em um arquivo CSV
        log_file_path = "log.csv"
        log_df.to_csv(log_file_path, mode='a', index=False, header=not os.path.exists(log_file_path))

    except Exception as e:
        print(f"Erro ao executar notebook {config['config']['notebook']}: {str(e)}")


def main():
    try:
        # Verificar se todos os arquivos necessários estao na pasta "input"
        if all(file in os.listdir(input_path) for file in required_files):
            processes = []

            # Separar as configurações e colocar para rodar em multiprocessing
            for config in configs:
                config_dict = [{'config':configs[config]}]
                p = multiprocessing.Process(
                target=run_notebook,
                args=(config_dict)
                )
                p.start()
                processes.append(p)

            # Aguarde todos os processos terminarem
        for p in processes:
            p.join()

        # Move os arquivos para a pasta output com a data atual no nome
        for file in required_files:
            if file != "Arq3.xlsx":
                input_file = os.path.join(input_path, file)
                output_file = os.path.join(output_path, f"{file.replace('.xlsx', '')}{datetime.now().strftime('%d%m%Y')}.xlsx")
                shutil.move(input_file, output_file)

        else:
            raise FileNotFoundError("Não foram encontrados todos os arquivos necessários na pasta input.")

    except Exception as e:
        print(f"Nenhum Arquivo Encontrado")

if __name__ == "__main__":
    main()