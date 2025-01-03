import pdb
import requests
import os
import pandas as pd
from tqdm import *
import psycopg2
from sqlalchemy import create_engine, text

defaultURL="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/objets-trouves-restitution/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
parameters={'host': 'localhost','database': 'webdata','user': 'postgres','password': 5432}
conn=psycopg2.connect(host=parameters['host'],database=parameters['database'],user=parameters['user'],password=parameters['password'])
engine=create_engine(f'postgresql://{parameters["user"]}:{parameters["password"]}@{parameters["host"]}/{parameters["database"]}')

#Méthode de transformation du fichier
def transformCSVFiles(fileName):
	print("Transformation du fichier")
	fileCsv=pd.read_csv(fileName,delimiter=";")

	#Suppression des colonnes non pertinentes pour notre exemple
	fileCsv=fileCsv.drop(["code_uic","type_enregistrement"],axis=1)

	fileCsv["date_decouverte"]=fileCsv["date"].map(lambda ele: str(ele).split("T")[0])
	fileCsv["date_restitution"]=fileCsv["date_et_heure_restitution"].map(lambda annee: "1970-01-01" if len(str(annee).split("T")[0])==3 else str(annee).split("T")[0])
	
	fileCsv["annee_restitution"]=fileCsv["date_restitution"].map(lambda date_restitution: str(date_restitution).split("-")[0])
	fileCsv["annee_decouverte"]=fileCsv["date_decouverte"].map(lambda date_decouverte: str(date_decouverte).split("-")[0])
	fileCsv.to_csv(fileName, sep=";",index=False,encoding="utf-8")
	
	print("Terminé")

#Méthode de suppression de l'ancien fichier
def deleteOldFile(fileName):
	os.remove(fileName)

#Méthode permettant de vérifier l'existence du fichier csv
def checkIfFileExists(fileName):
	isFileExists=False
	if os.path.exists(fileName):
		isFileExists=True

	return isFileExists

def downloadSNCFCSVFiles(fileName):
	print("Démarrage du téléchargement")
	csvFile=pd.read_csv(defaultURL,delimiter=";",low_memory=False)
	csvFile=csvFile.rename(columns={"Date":"date","Date et heure de restitution":"date_et_heure_restitution","Gare":"gare","Code UIC":"code_uic","Nature d'objets":"nature_objets","Type d'objets":"type_objets","Type d'enregistrement":"type_enregistrement"})
	csvFile.to_csv(fileName, sep=";",index=False,encoding="utf-8")
	print("Sauvegarde du fichier $fileName terminé ")
	return 1

#Méthode d'importation dans la base
def importDataIntoDataBase(fileName):
	print("Importation dans la base de données")
	csvFile=pd.read_csv(fileName,delimiter=";")
	csvFile.to_sql("objetsretrouves", engine, if_exists='replace', index=False)
	print("Fichier importé")
	print("Base de données mise avec succès")

#Téléchargement des fichiers csv
def starting(fileName):
	start=0
	while(start!=1):
		if checkIfFileExists(fileName)==True:
			message=input("Le fichier existe. Voulez-vous continuer oui/non ou o/n ? ")
			if (message.lower()=='oui') or (message.lower()=='o'):
				print("Suppression de l'ancien fichier et téléchargement d'une nouvelle version")
				deleteOldFile(fileName)
				print("Patientez...")
				start=downloadSNCFCSVFiles(fileName)
			elif (message.lower()=='non') or (message.lower()=='n'):
				start=1
			else:
				print("Reponse incorrecte. Réessayez SVP")
		else:
			start=downloadSNCFCSVFiles(fileName)

#Lancement

starting("download.csv")
transformCSVFiles("download.csv")
importDataIntoDataBase("download.csv")