from minio import Minio
import urllib.request
import pandas as pd
import sys
import os
import urllib.request
import re
from bs4 import BeautifulSoup

def main():
    grab_data()

def grab_data() -> None:
    """Récupère les données de New York Yellow Taxi pour janvier 2024

    Cette méthode télécharge le fichier des trajets des Yellow Taxi de janvier 2024 au format Parquet.
    Le fichier est enregistré dans le dossier "../../data/raw".
    """
    base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    folder_path = "../data/raw"
    
    # Téléchargement du contenu de la page
    print("Récupération du lien de téléchargement pour janvier 2024...")
    try:
        with urllib.request.urlopen(base_url) as response:
            page_content = response.read()
    except Exception as e:
        print(f"Erreur lors du téléchargement de la page principale : {e}")
        return

    # Parsing du contenu de la page avec BeautifulSoup
    soup = BeautifulSoup(page_content, 'html.parser')
    pattern = re.compile(r'yellow_tripdata_2024-01\.parquet')

    # Trouver le lien du fichier Parquet pour janvier 2024
    link = None
    for a in soup.find_all('a', href=True):
        if pattern.search(a['href']):
            link = a['href']
            break
    
    if not link:
        print("Le fichier Parquet pour janvier 2024 n'a pas été trouvé.")
        return

    # Télécharger le fichier
    file_name = link.split('/')[-1]
    file_path = os.path.join(folder_path, file_name)

    print(f"Téléchargement de {file_name}...")
    try:
        urllib.request.urlretrieve(link, file_path)
        print(f"{file_name} téléchargé avec succès.")
    except Exception as e:
        print(f"Échec du téléchargement de {file_name} : {e}")
    
# def grab_data() -> None:
#     """Grab the data from New York Yellow Taxi

#     This method download x files of the New York Yellow Taxi. 
    
#     Files need to be saved into "../../data/raw" folder
#     This methods takes no arguments and returns nothing.
#     """

#     base_url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
#     folder_path = "../../data/raw"
    
#     # Création du dossier s'il n'existe pas
#     if not os.path.exists(folder_path):
#         os.makedirs(folder_path)
    
#     # Téléchargement du contenu de la page
#     print("Récupération des liens de téléchargement depuis la page principale...")
#     try:
#         with urllib.request.urlopen(base_url) as response:
#             page_content = response.read()
#     except Exception as e:
#         print(f"Erreur lors du téléchargement de la page principale : {e}")
#         return

#     # Parsing du contenu de la page avec BeautifulSoup
#     soup = BeautifulSoup(page_content, 'html.parser')
#     pattern = re.compile(r'yellow_tripdata_2024-\d{2}\.parquet')

#     # Trouver tous les liens de fichiers Parquet pour 2024
#     links = [a['href'] for a in soup.find_all('a', href=True) if pattern.search(a['href'])]
    
#     if not links:
#         print("Aucun fichier Parquet trouvé pour l'année 2024.")
#         return

#     # Téléchargement des fichiers
#     for link in links:
#         file_name = link.split('/')[-1]
#         file_path = os.path.join(folder_path, file_name)

#         # Télécharger le fichier
#         print(f"Téléchargement de {file_name}...")
#         try:
#             urllib.request.urlretrieve(link, file_path)
#             print(f"{file_name} téléchargé avec succès.")
#         except Exception as e:
#             print(f"Échec du téléchargement de {file_name} : {e}")


def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "NOM_DU_BUCKET_ICI"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")

if __name__ == '__main__':
    sys.exit(main())
