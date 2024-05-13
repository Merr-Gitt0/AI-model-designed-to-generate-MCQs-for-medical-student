import pytesseract
# Set the path to the Tesseract executable (change it based on your installation path)
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
import fitz
from PIL import Image
import pandas as pd
import cx_Oracle
import schedule
import time
from unittest.mock import patch
from datetime import datetime
from __future__ import print_function
from google.oauth2 import service_account
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload
from unicodedata import normalize
import os
import io
import re
import string
import csv
from tqdm import tqdm
import unittest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import StringIO
from googleapiclient.errors import HttpError
from googleapiclient.http import HttpRequest, MediaIoBaseDownload
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller


data = pd.read_csv("question_data.csv")
dsn_tns = cx_Oracle.makedsn('localhost','1522','xe')
conn = cx_Oracle.connect(user='etl',password='sabrina',dsn=dsn_tns)
c = conn.cursor()
has_logs = False


scope = ['https://www.googleapis.com/auth/drive.readonly']

credentials = ServiceAccountCredentials.from_json_keyfile_name('bda96592-5a7e45fabb00.json', scope)

service = build('drive', 'v3', credentials=credentials)

target_directory = 'drive'


# Chaîne de connexion à la base de données Oracle
CONN_STRING = 'etl/sabrina@localhost:1522/xe'

connection = cx_Oracle.connect(CONN_STRING)
cursor = connection.cursor()
directory_path = 'drive1'

# ---------------------->  Extract Data <----------------------#
# --------------- > WebScrapping code: 


chromedriver_autoinstaller.install()
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run Chrome in headless mode (no GUI)
def create_quiz(quiz_value):
    question_list = []
    try:
        # Create a WebDriver instance with the Chrome options
        driver = webdriver.Chrome(options=chrome_options)

        # Navigate to the login page
        driver.get("https://qcmology.net/login")

        # Wait until the email input field is visible
        email_input = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located((By.NAME, "email"))
        )

        # Enter the email address
        email_input.send_keys("km_karim@esi.dz")

        # Find the password input field and enter the password
        password_input = driver.find_element(By.NAME, "password")
        password_input.send_keys("360312karim2020**")

        # Find and click the login button
        login_button = driver.find_element(By.ID, ":r2:")
        login_button.click()
        time.sleep(5)  # Wait for 5 seconds for demonstration purposes
        # Find and click on the link to navigate to the quiz building page
        try:
            # Wait for the element to be located and become clickable
            button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".MuiButtonBase-root.MuiIconButton-root.MuiIconButton-sizeMedium.css-1utsr5y"))
            )

            # Click the button
            button.click()

            element_with_text = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, '//div[@class="MuiListItemText-root css-xdiy5h" and text()="Créer un quiz"]'))
            )
            element_with_text.click()
            time.sleep(5) 
            print(driver.title)
            page_info = {
                "title": driver.title,
                "current_url": driver.current_url,
                "page_source": driver.page_source,
                "cookies": driver.get_cookies()
                # You can add more information here as needed
            }
            checkboxes = driver.find_elements(By.XPATH, '//input[@type="checkbox"]')
            for checkbox in checkboxes:
                if checkbox.get_attribute('value') == quiz_value: 
                    if not checkbox.is_selected():  
                        checkbox.click() 

            input_element = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, '.dropdown__input'))
            )
            input_element.clear()
            input_element.send_keys("100")
            button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'span.button_construire'))
            )
            
            # Click on the element
            button.click()
            time.sleep(5) 
            
            for _ in range(100):
                responses = []
                question_element = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//div[@class="question_informations"]/div[2]'))
                )
                question_text = question_element.text
                print("Question Text:", question_text)

                options_list = driver.find_element(By.CLASS_NAME, 'options')
                option_items = options_list.find_elements(By.CLASS_NAME, 'option-item')
                options_text = [option_item.text.strip() for option_item in option_items]

                for index, option_text in enumerate(options_text, start=1):
                    print(f"Option {index}: {option_text}")
                for index, option_item in enumerate(option_items, start=1):
                    option_item.click()
                button_verifier = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.control_button.css-150vkwt'))
                )
                button_verifier.click()
                time.sleep(5)
                option_items = driver.find_elements(By.CLASS_NAME, "option-item")
                for option_item in option_items:
                    class_attribute = option_item.get_attribute("class")
                    if "wrong" in class_attribute:
                        responses.append('Faux')
                    else:
                        responses.append('Vrai')
                question_data = {
                    "question": question_text,
                    "options": options_text,
                    "reponses": responses
                }
                question_list.append(question_data)

                buttons = driver.find_elements(By.CSS_SELECTOR, '.MuiButtonBase-root.MuiIconButton-                           root.MuiIconButton-sizeMedium.css-4cxmk4')
                print(len(buttons))
                button = buttons[2]
                print(button)
                button.click()
                time.sleep(5)

        except Exception as e:
            print("An error occurred:", e)

        #print("Title of the webpage after navigating to quiz building page:", driver.title)

    finally:
        # Quit the WebDriver session
        driver.quit()
        return question_list

question_list = create_quiz("21")  # Pass the quiz value as a parameter
questions = create_quiz("60")
questionadd= create_quiz("49")

# ----------------------> extract data from PDFs QCMS :


scope = ['https://www.googleapis.com/auth/drive.readonly']

credentials = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/slama/Downloads/bda96592-5a7e45fabb00.json', scope)

service = build('drive', 'v3', credentials=credentials)

target_directory = 'C:/Users/slama/Downloads/test9'

def download_drive_contents(service, drive_id, target_directory, exclude_folder='Cours'):
    results = service.files().list(
        q=f"'{drive_id}' in parents",
        fields="files(name, id, mimeType, parents)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            file_name = item['name']
            file_id = item['id']
            mime_type = item['mimeType']
            parents = item.get('parents', [])

            if any(exclude_folder in parents_list for parents_list in parents):
                print(f'Skipping file in folder "{exclude_folder}": {file_name}')
                continue

            if mime_type == 'application/vnd.google-apps.folder':
                # Recursive call for subfolders
                subfolder_path = os.path.join(target_directory, normalize('NFKD', file_name).encode('ascii', 'ignore').decode('ascii'))
                os.makedirs(subfolder_path, exist_ok=True)
                download_drive_contents(service, file_id, subfolder_path, exclude_folder)
            else:
                # Download and convert file
                download_and_convert_file(service, file_id, file_name, target_directory)



def download_and_convert_file(service, file_id, file_name, target_directory):
    if file_name.lower().endswith('.pdf'):
        # Download the PDF file
        full_pdf_path = os.path.join(target_directory, file_name)
        download_file(service, file_id, full_pdf_path)

        # Convert PDF to text
        output_txt_path = os.path.join(target_directory, os.path.splitext(file_name)[0] + '.txt')
        extract_text_from_scanned_pdf(full_pdf_path, output_txt_path)

        # Delete the PDF file after converting
        os.remove(full_pdf_path)

def download_file(service, file_id, full_path):
    print(f'Downloading: {full_path}')
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(full_path, mode='wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%")

    print(f'Saved: {full_path}')

def extract_text_from_scanned_pdf(pdf_path, output_txt_path):
    # Open the PDF file
    pdf_document = fitz.open(pdf_path)

    # Initialize a list to store the extracted text from each page
    extracted_text = []

    for page_number in range(pdf_document.page_count):
        # Extract the image from the page
        page = pdf_document[page_number]
        pix = page.get_pixmap()

        # Use Pillow to convert the image to a format that Tesseract can handle
        image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        image_path = f"page_{page_number + 1}.png"

        # Save the image to a file
        image.save(image_path)

        # Use Tesseract to extract text from the image
        text = pytesseract.image_to_string(image)

        # Append the extracted text to the list
        extracted_text.append(text)

    # Save the extracted text to a text file
    with open(output_txt_path, 'w', encoding='utf-8') as txt_file:
        for text in extracted_text:
            txt_file.write(f"\n{text}\n")

    # Close the PDF document
    pdf_document.close()



# ----------------------> Data Transformation <----------------------#

# ----------------------> Transform Data from Webscrapping 

    question_data=question_list
    question_data2=questions
    question_data3=questionsadd
    question_data_with_module = []
    for item in question_data:
        item_with_module = item.copy()  # Create a shallow copy of the item
        item_with_module["Module"] = "neuro_chirurgie"
        question_data_with_module.append(item_with_module)
    for item in question_data2:
        item_with_module = item.copy()  # Create a shallow copy of the item
        item_with_module["Module"] = "nephrologie"
        question_data_with_module.append(item_with_module)
    for item in question_data2:
        item_with_module = item.copy()  # Create a shallow copy of the item
        item_with_module["Module"] = "urgence_medicale"
        question_data_with_module.append(item_with_module)
    # Define the CSV file name
    csv_file = "question_data.csv"
    # Write the data to the CSV file
    with open(csv_file, "w", newline="", encoding="utf-8") as file:
        # Create a CSV writer object
        writer = csv.DictWriter(file, fieldnames=question_data_with_module[0].keys())

        # Write the header
        writer.writeheader()

        # Write the data to the CSV file
        for item in question_data_with_module:
            writer.writerow(item)
       

    print("CSV file has been created successfully.")

#------------------> transform data from QCMs 

def extract_qcm(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    questions = []
    current_question = ""
    choices = []
    current_cas_number = None

    for line in lines:
        line = line.strip()

        cas_match = re.match(r'^CAS CLINIQUE n° (\d+)', line)
        if cas_match:
            current_cas_number = int(cas_match.group(1))
            continue  # Skip the line with the "CAS CLINIQUE" header

        if line.endswith('?') or line.endswith(':'):
            # Check if there's an ongoing question and choices
            if current_question and choices:
                questions.append({
                    "cas_number": current_cas_number,
                    "question": current_question,
                    "choices": choices
                })
            # Start a new question
            current_question = line
            choices = []
        elif line:
            # Check if a choice line, and add it to choices
            choice_match = re.match(r'^[A-E]\s*-\s*', line) or re.match(r'^[A-E]\s*.\s*', line)
            if choice_match:
                choices.append(line[choice_match.end():].strip())
            # If not a choice line, add it to the ongoing question
            else:
                current_question += " " + line

    # Add the last question and choices
    if current_question and choices:
        if len(choices) == 4:
            questions.append({
                "cas_number": current_cas_number,
                "question": current_question,
                "choices": choices
            })

    return pd.DataFrame(questions)


def extract_cas_canonic(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    cas_canonic_list = []
    current_cas_canonic = ""
    current_cas_number = None

    for line in lines:
        line = line.strip()

        cas_match = re.match(r'^CAS CLINIQUE n°\s*(\d+)', line)
        if cas_match:
            if current_cas_canonic:  # Save the previous canonic if it exists
                cas_canonic_list.append({
                    "cas_number": current_cas_number,
                    "cas_canonic": current_cas_canonic
                })
            current_cas_canonic = line
            current_cas_number = int(cas_match.group(1))
        elif line == ".":
            current_cas_canonic += line  # Include the period
            cas_canonic_list.append({
                "cas_number": current_cas_number,
                "cas_canonic": current_cas_canonic
            })
            current_cas_canonic = ""
        elif current_cas_canonic:  # Append lines to the current canonic
            current_cas_canonic += " " + line

    # Add the last cas_canonic
    if current_cas_canonic:
        cas_canonic_list.append({
            "cas_number": current_cas_number,
            "cas_canonic": current_cas_canonic
        })

    return pd.DataFrame(cas_canonic_list)

def merge_dataframes(qcm_df, cas_canonic_df):
    # Merge DataFrames based on "cas_number" column
    merged_df = pd.merge(qcm_df, cas_canonic_df, on='cas_number', how='left')

    return merged_df



def extract_responses(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    response_matches = re.findall(r'QCM\s*(\d+)\s*:\s*([A-E]+)', content)

    responses_list = []
    for question_number, response in response_matches:
        response_array = [char in response for char in 'ABCDE']
        responses_list.append({
            
            "response": response_array
        })

    return pd.DataFrame(responses_list)


def merge_dataframes(qcm_df, responses_df):
    # Concatenate DataFrames along the columns (axis=1)
    merged_df = pd.concat([qcm_df, responses_df], axis=1)
    return merged_df


def transform(file_path_qcm, merged_output_path):
    # Call the extract functions and convert to DataFrames
    first = extract_qcm(file_path_qcm)
    second = extract_cas_canonic(file_path_qcm)
    third = merge_dataframes(first, second)
    extracted_data_responses = extract_responses(file_path_qcm)

    # Call the merge_dataframes function
    merged_data = merge_dataframes(third, extracted_data_responses)
    
    # Extract year and module from the file path
    year, module = extract_year_module(file_path_qcm)

    # Add year and module columns to the DataFrame
    merged_data['year'] = year
    merged_data['module'] = module

    if 'cas_number' in merged_data.columns:
    # Drop unnecessary columns
        merged_data = merged_data.drop(columns=['cas_number'])


        if 'cas_canonic' in merged_data.columns:
            merged_data['cas_canonic'].fillna('', inplace=True)  # Replace NaN values with an empty string
            merged_data.to_csv(merged_output_path, index=False, encoding='utf-8')

       
    merged_data.to_csv(merged_output_path, index=False, encoding='utf-8')



def extract_year_module(file_path):
    parts = file_path.split(os.path.sep)
    print(parts)
    # Check if 'QCMs' is in the parts list
    if 'QCMs' in parts:
        year_index = 1
        module_index = 2
        year = parts[year_index]
        module = parts[module_index]

        return year, module

    else:
        return '6', 'incomue'



# Initialize an empty DataFrame
merged_results = pd.DataFrame(columns=['year', 'module'])

# data trasformation function
def process_folder(root_folder):
    global merged_results  # Declare the DataFrame as global to modify it within the function

    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".txt"):
                file_path = os.path.join(root, file)
                # Call the transform function
                transform(file_path, "merged_output.csv")
                
                # Print the file path to check if it's correct
                print(f"Reading: {os.path.splitext(file_path)[0] + '_output.csv'}")
                
                # Load the transformed data into a DataFrame
                df = pd.read_csv("merged_output.csv")
                # Append the DataFrame to the merged_results DataFrame
                merged_results = pd.concat([merged_results, df], ignore_index=True)





 

#------------------------->Load data in Oracle database<-------------------------#
                


#------------------------->Creating the database if has_logs=False
if has_logs ==False:  
    c.execute('CREATE SEQUENCE cas_Seq')
    c.execute('CREATE SEQUENCE modules_Seq')
    c.execute('CREATE SEQUENCE cours_Seq')
    c.execute('CREATE SEQUENCE qcms_Seq')
    c.execute('CREATE SEQUENCE questions_Seq')
    c.execute('CREATE SEQUENCE choix_Seq')
    c.execute('CREATE SEQUENCE log_Seq')
    c.execute('CREATE TABLE modules (moduleID NUMBER PRIMARY KEY,nom VARCHAR2(255),annee NUMBER)')
    c.execute('CREATE TABLE cours (coursID NUMBER PRIMARY KEY, file_name VARCHAR2(255),pdf_blob BLOB,module_id NUMBER,CONSTRAINT c1 FOREIGN KEY (module_id) REFERENCES modules (moduleID))')
    c.execute('CREATE TABLE qcms (qcmID NUMBER PRIMARY KEY,module NUMBER ,CONSTRAINT c2 FOREIGN KEY (module) REFERENCES modules (moduleID))')
    c.execute('CREATE TABLE cas_cliniques (casID NUMBER PRIMARY KEY,cas_text CLOB)')
    c.execute('CREATE TABLE questions (questionID NUMBER PRIMARY KEY,questionText CLOB,qcm NUMBER,cas_clinique NUMBER,CONSTRAINT c3 FOREIGN KEY (qcm) REFERENCES qcms (qcmID), CONSTRAINT c5 FOREIGN KEY (cas_clinique) REFERENCES cas_cliniques(casID))')
    c.execute('CREATE TABLE choix (choixID NUMBER PRIMARY KEY,choix_text VARCHAR2(255),is_reponse NUMBER(1),question NUMBER,letter VARCHAR2(255),CONSTRAINT c4 FOREIGN KEY (question) REFERENCES questions (questionID))')
    c.execute('CREATE TABLE logs (logID NUMBER PRIMARY KEY,log DATE)')
    c.execute('''
    CREATE OR REPLACE TRIGGER modules_trigger
    BEFORE INSERT ON modules
    FOR EACH ROW
    BEGIN
        IF :new.moduleID IS NULL THEN
            :new.moduleID := modules_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER log_trigger
    BEFORE INSERT ON logs
    FOR EACH ROW
    BEGIN
        IF :new.logID IS NULL THEN
            :new.logID := log_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER cours_trigger
    BEFORE INSERT ON cours
    FOR EACH ROW
    BEGIN
        IF :new.coursID IS NULL THEN
            :new.coursID := cours_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER qcm_trigger
    BEFORE INSERT ON qcms
    FOR EACH ROW
    BEGIN
        IF :new.qcmID IS NULL THEN
            :new.qcmID := qcms_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER cas_trigger
    BEFORE INSERT ON cas_cliniques
    FOR EACH ROW
    BEGIN
        IF :new.casID IS NULL THEN
            :new.casID := cas_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER questions_trigger
    BEFORE INSERT ON questions
    FOR EACH ROW
    BEGIN
        IF :new.questionID IS NULL THEN
            :new.questionID := questions_Seq.NEXTVAL;
        END IF;
    END;
    ''')
    c.execute('''
    CREATE OR REPLACE TRIGGER choix_trigger
    BEFORE INSERT ON choix
    FOR EACH ROW
    BEGIN
        IF :new.choixID IS NULL THEN
            :new.choixID := choix_Seq.NEXTVAL;
        END IF;
    END;
    ''')

#------------------------->loading the webscapping results
module_nom = ""
new_qcm_id = c.var(cx_Oracle.NUMBER)
new_module_id = c.var(cx_Oracle.NUMBER)
for index, row in data.iterrows():
    new_question_id = c.var(cx_Oracle.NUMBER)
    
    if(row.Module != module_nom):
        module_nom = row.Module
        annee = 6
        c.execute("""insert into modules (nom,annee) values (:module_nom,:annee) returning moduleID into :id""",id = new_module_id,module_nom = module_nom,annee = annee)
        id_module_array = new_module_id.getvalue()
        id_module = id_module_array[0]
        c.execute("""insert into qcms (module) values (:id_module) returning qcmID into :id""",id = new_qcm_id,id_module = id_module)
        qcm_id_array = new_qcm_id.getvalue()
        qcm_id = qcm_id_array[0]
        
    text_quest = row.question
    c.execute("""insert into questions (questiontext, qcm) values (:text_quest,:qcm_id) returning questionID into :id""",id = new_question_id,text_quest=text_quest, qcm_id=qcm_id)
    question_id_array = new_question_id.getvalue()
    question_id = question_id_array[0]
    options_list = row.options.replace('[', '').replace(']', '').replace("'", '').split(',')
    for i, option in enumerate(options_list):
        print(option,i)
        j,_ , choix_txt = option.partition('\\')
        choix_txt = choix_txt[1:]
        # Now 'letter' contains the letter (A, B, C, D, E) and 'description' contains the corresponding description
        print(f"Letter: {j}, Description: {choix_txt}")
        if row.reponses[i] == 'Faux':
            is_correct = 0
        else:
            is_correct = 1
        
        c.execute("""insert into choix (choix_text, is_reponse, question, letter) values (:choix_txt,:is_correct,:qcm_id,:j)""",choix_txt = choix_txt,is_correct = is_correct,qcm_id=qcm_id,j=j)
c.execute("COMMIT")

#------------------------->Loading the folder extraction results

alphabet_lower_list = list(string.ascii_lowercase)
alphabet = list(string.ascii_uppercase)
module_nom = ""
new_qcm_id = c.var(cx_Oracle.NUMBER)
new_module_id = c.var(cx_Oracle.NUMBER)
for index, row in data2.iterrows():
    options_list = row.choices.replace('[', '').replace(']', '').replace("'", '').split(',')
    if len(row.module)>5 and len(options_list)<9 : 
        new_question_id = c.var(cx_Oracle.NUMBER)
        new_cas_id = c.var(cx_Oracle.NUMBER)
        if(row.module != module_nom and row.module != ""):
            module_nom = row.module
            annee = row.year
            c.execute(f"SELECT * FROM modules WHERE nom = :module_nom AND annee = :annee", module_nom=module_nom, annee = annee)
            result = c.fetchall()
            print(result)
            if result : 
                id_module = result[0][0]
            else:
                c.execute("""insert into modules (nom,annee) values (:module_nom,:annee) returning moduleID into :id""",id = new_module_id,module_nom = module_nom,annee = annee)
                id_module_array = new_module_id.getvalue()
                id_module = id_module_array[0]
            
            c.execute("""insert into qcms (module) values (:id_module) returning qcmID into :id""",id = new_qcm_id,id_module = id_module)
            qcm_id_array = new_qcm_id.getvalue()
            qcm_id = qcm_id_array[0]

        if row.cas_canonic != "" and row.cas_canonic != "." and row.cas_canonic != ". ":
            cas_txt = row.cas_canonic
            c.execute("""insert into cas_cliniques (cas_text) values (:cas_txt) returning casID into :id""",id = new_cas_id,cas_txt=cas_txt)
            cas_id_array = new_cas_id.getvalue()
            cas_id = cas_id_array[0]
            text_quest = row.question
            c.execute("""insert into questions (questiontext, qcm, cas_clinique) values (:text_quest,:qcm_id, :cas_id) returning questionID into :id""",id = new_question_id,text_quest=text_quest, qcm_id=qcm_id,cas_id = cas_id)
        else : 
            text_quest = row.question
            c.execute("""insert into questions (questiontext, qcm) values (:text_quest,:qcm_id) returning questionID into :id""",id = new_question_id,text_quest=text_quest, qcm_id=qcm_id)
    
        question_id_array = new_question_id.getvalue()
        question_id = question_id_array[0]
        options_list = row.choices.replace('[', '').replace(']', '').replace("'", '').split(',')
        if row.response != "":
            reponse_list = row.response.replace('[', '').replace(']', '').replace("'", '').split(',')
        for i, option in enumerate(options_list):
            choix_txt = options_list[i]
            if i < len(alphabet):
                j = alphabet[i]
            else :
                j = f"C{i + 1}"
            
            print(option,i)
            if row.response != "":
                if reponse_list == 'Faux':
                    is_correct = 0
                else:
                    is_correct = 1
                c.execute("""insert into choix (choix_text, is_reponse, question, letter) values (:choix_txt,:is_correct,:qcm_id,:j)""",choix_txt = choix_txt,is_correct = is_correct,qcm_id=qcm_id,j=j)
            else:
                c.execute("""insert into choix (choix_text, question, letter) values (:choix_txt,:qcm_id,:j)""",choix_txt = choix_txt,qcm_id=qcm_id,j=j)
c.execute("COMMIT")
c.close()
conn.close()
    


#------------------------->  COURS_LOADING <-------------------------#

def download_drive_contents(service, drive_id, target_directory):
    results = service.files().list(
        q=f"'{drive_id}' in parents",
        fields="files(name, id, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()

    items = results.get('files', [])

    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            file_name = item['name']
            file_id = item['id']
            mime_type = item['mimeType']

            if mime_type == 'application/vnd.google-apps.folder':
                if file_name != 'QCMs':
                    # Recursive call for subfolders
                    subfolder_path = os.path.join(target_directory, normalize('NFKD', file_name).encode('ascii', 'ignore').decode('ascii'))
                    os.makedirs(subfolder_path, exist_ok=True)
                    download_drive_contents(service, file_id, subfolder_path)
            else:
                # Download and convert file
                download_and_convert_file(service, file_id, file_name, target_directory)

def download_and_convert_file(service, file_id, file_name, target_directory):
    if file_name.lower().endswith('.pdf'):
        # Download the PDF file
        full_pdf_path = os.path.join(target_directory, file_name)
        download_file(service, file_id, full_pdf_path)


def download_file(service, file_id, full_path):
    print(f'Downloading: {full_path}')
    request = service.files().get_media(fileId=file_id)
    with tqdm.wrapattr(open(full_path, 'wb'), 'write', miniters=1,
                        total=int(request.headers.get('content-length', 0)),
                        desc=full_path) as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            try:
                status, done = downloader.next_chunk()
            except Exception as e:
                print(f"An error occurred: {e}")
                done = True
            else:
                if status:
                    fh.flush()
                    print(f"Download {int(status.progress() * 100)}%")

    print(f'Saved: {full_path}')





cursor.execute('DROP SEQUENCE pdf_file_seq')
cursor.execute('CREATE SEQUENCE pdf_file_seq START WITH 1 INCREMENT BY 1')

# Iterate over the subdirectories and files in the directory
for root, dirs, files in os.walk(directory_path):
    for file_name in files:
        if file_name.endswith('.pdf'):
            # Obtenir le nom du répertoire précédant immédiatement le dernier répertoire
             
            # Utilisation du parent_folder pour obtenir l'id du module
            
            module_name = os.path.split(os.path.split(root)[0])[1]
            
            annee_part = os.path.split(os.path.split(root)[0])[0]
            annee = int(annee_part.split('\\')[-1])
            print("module_name:",module_name )
            print("file_name:",file_name )

            # Create full file path
            file_path = os.path.join(root, file_name)
            
            # Insérer une nouvelle ligne dans la table
            last_id = cursor.var(cx_Oracle.NUMBER)

            new_module_id = cursor.var(cx_Oracle.NUMBER)
            # Exécuter la requête SQL
            cursor.execute("SELECT MODULEID FROM MODULES WHERE NOM = :module_name AND ANNEE = :annee", {"module_name":module_name,"annee":annee})

            # Récupérer le résultat de la requête
            idd = cursor.fetchone()
            if idd : 
                idd = idd[0]
            else:
                cursor.execute("""insert into modules (nom,annee) values (:module_name,:annee) returning moduleID into :id""",id = new_module_id,module_name = module_name,annee = annee)
                id_module_array = new_module_id.getvalue()
                idd = id_module_array[0]
            idd = int(idd);
            print(idd)
            # Lire le contenu binaire du fichier PDF
            #with open(file_path, 'rb') as file:
            #    pdf_content = file.read()
            
            # Créer un objet BLOB vide
            pdf_blob = cursor.var(cx_Oracle.BLOB)
            cursor.execute("INSERT INTO COURS (coursID, file_name, pdf_blob, module_id) VALUES (pdf_file_seq.nextval, :file_name, EMPTY_BLOB(), :module_id) RETURNING coursID, pdf_blob INTO :last_id, :pdf_blob ", file_name=file_name, module_id=idd, last_id=last_id, pdf_blob=pdf_blob)

 # Lire le contenu du fichier PDF et le mettre dans l'objet BLOB
            with open(file_path, 'rb') as file:
                  file_content = file.read()
                  pdf_blob.setvalue(0, file_content)
            last_insert_id_array = last_id.getvalue()
            last_insert_id= last_insert_id_array[0]
            # Afficher la dernière valeur générée
            print("Last Insert ID:", last_insert_id)
            
          # Mettre à jour l'enregistrement avec l'objet BLOB contenant le contenu du fichier PDF
            cursor.execute("UPDATE COURS SET pdf_blob = :pdf_blob WHERE coursID = :id", pdf_blob=pdf_blob, id=last_insert_id)
            
            
            # Mettre à jour l'objet BLOB vide avec le contenu binaire du fichier
            #pdf_blob.setvalue(0, pdf_content)

            print("fin")
            # Récupérer la dernière valeur générée par la séquence
            
            
# Commit the transaction
connection.commit()

# Close the cursor and the connection
cursor.close()
connection.close()



#------------------------->  schedule <-------------------------#
#------------------------->  schedule function
def job():
    
    query = "SELECT log FROM (SELECT log FROM logs ORDER BY logID DESC) WHERE ROWNUM = 1"
    try:
        c.execute(query)
        result = c.fetchone()
        if result is not None:
            has_logs = True
            last_log = c.fetchone()[0]
            print("Last log record:", last_log)
            root_drive_id = "0ADDgdqb0__kwUk9PVA" #replace this with id of the our shared drive
            download_drive_contents(service, root_drive_id, target_directory, last_log)
            #extract
            #transform(target_directory, output_)
            #load

    except cx_Oracle.DatabaseError as e:
        print("An error occurred:", e)
        has_logs=False
        last_log= 0
        root_drive_id = "0ADDgdqb0__kwUk9PVA" 
        download_drive_contents(service, root_drive_id, target_directory, last_log)






#------------------------->  Unit tests <-------------------------#
#------------------------->  Unit extraction test :

#------------------------->  WebScrapping

class TestWebScraping(unittest.TestCase):
    def setUp(self):
        
        self.driver = webdriver.Chrome()
        self.driver.get("https://qcmology.net/login")

    def tearDown(self):
        
        self.driver.quit()

    def test_create_quiz(self):
        
        question_list = create_quiz("21")
        self.assertTrue(isinstance(question_list, list))
        self.assertTrue(len(question_list) > 0)
        

        
#------------------------->  PDF extraction 


class TestGoogleDriveFunctions(unittest.TestCase):



    def test_download_drive_contents(self):
        # Mock the service.files().list method to return a sample list of files
        with patch("googleapiclient.discovery.Resource.files") as mock_files:
            mock_files.return_value.list.return_value.execute.return_value = {
                "files": [
                    {"name": "file1.pdf", "id": "123", "mimeType": "application/pdf", "parents": []},
                    {"name": "file2.txt", "id": "456", "mimeType": "text/plain", "parents": []},
                    {"name": "folder1", "id": "789", "mimeType": "application/vnd.google-apps.folder", "parents": []},
                ]
            }

            # Mock the download_and_convert_file function
            with patch("__main__.download_and_convert_file") as mock_download_and_convert:
                # Mock the download_drive_contents function recursively for subfolders
                with patch("__main__.download_drive_contents") as mock_download_contents:
                    download_drive_contents(service, "root_drive_id", "target_directory")

                    # Assertions
                    mock_download_and_convert.assert_called_once_with(service, "123", "file1.pdf", "target_directory")
                    mock_download_contents.assert_called_once_with(service, "789", os.path.join("target_directory", "folder1"), exclude_folder='Cours')

    def test_download_and_convert_file_pdf(self):
        # Mock the download_file and extract_text_from_scanned_pdf functions
        with patch("__main__.download_file") as mock_download_file:
            with patch("__main__.extract_text_from_scanned_pdf") as mock_extract_text:
                download_and_convert_file(service, "file_id", "file.pdf", "target_directory")

                # Assertions
                mock_download_file.assert_called_once_with(service, "file_id", os.path.join("target_directory", "file.pdf"))
                mock_extract_text.assert_called_once_with(os.path.join("target_directory", "file.pdf"), os.path.join("target_directory", "file.txt"))

    def test_download_and_convert_file_other(self):
        # Mock the download_file function
        with patch("__main__.download_file") as mock_download_file:
            download_and_convert_file(service, "file_id", "file.txt", "target_directory")

            # Assertions
            mock_download_file.assert_called_once_with(service, "file_id", os.path.join("target_directory", "file.txt"))

    def test_download_file(self):
        # Mock the service.files().get_media method to simulate download
        with patch("googleapiclient.discovery.Resource.files") as mock_files:
            with patch("googleapiclient.http.MediaIoBaseDownload") as mock_media_download:
                request = HttpRequest()
                request.uri = "https://example.com"
                request.body = "dummy_body"
                media_download = MediaIoBaseDownload(StringIO("dummy_content"), request)

                # Simulate a successful download
                media_download.next_chunk.return_value = (None, True)

                mock_files.return_value.get_media.return_value.execute.return_value = media_download

                # Call the function
                download_file(service, "file_id", "target_file.txt")

                # Assertions
                mock_files.return_value.get_media.assert_called_once_with(fileId="file_id")
                mock_media_download.assert_called_once_with(StringIO(), request)
                media_download.next_chunk.assert_called_once()

    def test_extract_text_from_scanned_pdf(self):
        # Mock the fitz.open, Image.frombytes, and pytesseract.image_to_string functions
        with patch("__main__.fitz.open") as mock_fitz_open:
            with patch("__main__.Image.frombytes") as mock_image_frombytes:
                with patch("__main__.pytesseract.image_to_string") as mock_image_to_string:
                    # Simulate a PDF with two pages, each containing "Page 1" and "Page 2" as text
                    mock_page_1 = mock_fitz_open.return_value.__getitem__.return_value
                    mock_page_1.get_pixmap.return_value = mock_image_frombytes.return_value
                    mock_page_1.get_pixmap.return_value.width = 100
                    mock_page_1.get_pixmap.return_value.height = 50
                    mock_page_1.get_pixmap.return_value.samples = b"dummy_samples_page_1"

                    mock_page_2 = mock_fitz_open.return_value.__getitem__.return_value
                    mock_page_2.get_pixmap.return_value = mock_image_frombytes.return_value
                    mock_page_2.get_pixmap.return_value.width = 100
                    mock_page_2.get_pixmap.return_value.height = 50
                    mock_page_2.get_pixmap.return_value.samples = b"dummy_samples_page_2"

                    # Call the function
                    extract_text_from_scanned_pdf("dummy_pdf_path.pdf", "dummy_output.txt")

                    # Assertions
                    mock_fitz_open.assert_called_once_with("dummy_pdf_path.pdf")
                    self.assertEqual(mock_image_frombytes.call_count, 2)
                    mock_image_to_string.assert_called_once_with(mock_image_frombytes.return_value)



#------------------------->  Unit transformation test :

         
class TestQcmFunctions(unittest.TestCase):



    def test_extract_qcm(self):
        file_path = "sample_qcm.txt"
        df = extract_qcm(file_path)
        
        self.assertListEqual(df.columns.tolist(), ["cas_number", "question", "choices"])
       

    def test_extract_cas_canonic(self):
        file_path = "sample_cas_canonic.txt"
        df = extract_cas_canonic(file_path)
        
        self.assertListEqual(df.columns.tolist(), ["cas_number", "cas_canonic"])
        

    def test_merge_dataframes(self):
        qcm_df = pd.DataFrame({"cas_number": [1, 2], "question": ["Q1", "Q2"], "choices": [["A", "B", "C", "D"], ["E", "F", "G", "H"]]})
        cas_canonic_df = pd.DataFrame({"cas_number": [1, 2], "cas_canonic": ["Cas1", "Cas2"]})
        merged_df = merge_dataframes(qcm_df, cas_canonic_df)
        self.assertListEqual(merged_df.columns.tolist(), ["cas_number", "question", "choices", "cas_canonic"])
       

    def test_extract_responses(self):
        file_path = "sample_responses.txt"
        df = extract_responses(file_path)
    
        self.assertListEqual(df.columns.tolist(), ["response"])
        

    def test_merge_dataframes_responses(self):
        qcm_df = pd.DataFrame({"cas_number": [1, 2], "question": ["Q1", "Q2"], "choices": [["A", "B", "C", "D"], ["E", "F", "G", "H"]]})
        responses_df = pd.DataFrame({"response": [[True, False, True, False], [False, True, False, True]]})
        merged_df = merge_dataframes(qcm_df, responses_df)
        self.assertEqual(merged_df.shape, (2, 4))
        self.assertListEqual(merged_df.columns.tolist(), ["cas_number", "question", "choices", "response"])
        
    def test_transform(self):
        file_path_qcm = "sample_transform_qcm.txt"
        merged_output_path = "sample_transform_output.csv"
        transform(file_path_qcm, merged_output_path)
        df = pd.read_csv(merged_output_path)
        
        self.assertListEqual(df.columns.tolist(), ["question", "choices", "response", "year", "module"])
        

    def test_extract_year_module(self):
        file_path = "sample_files/QCMs/2022/Module1/QCM.txt"
        year, module = extract_year_module(file_path)
        self.assertEqual(year, "2022")
        self.assertEqual(module, "Module1")


#---------------------------> Data base creation test <---------------------------# 

class TestDatabaseScript(unittest.TestCase):

    def setUp(self):
        # Set up the database connection
        self.dsn_tns = cx_Oracle.makedsn('localhost', '1522', 'xe')
        self.conn = cx_Oracle.connect(user='etl', password='sabrina', dsn=self.dsn_tns)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        # Clean up resources
        self.cursor.close()
        self.conn.close()

    def test_database_connection(self):
        
        self.assertIsNotNone(self.conn)

    def test_table_creation(self):
        
        tables = ['modules', 'cours', 'qcms', 'cas_cliniques', 'questions', 'choix', 'logs']
        sequences = ['cas_Seq', 'modules_Seq', 'cours_Seq', 'qcms_Seq', 'questions_Seq', 'choix_Seq', 'log_Seq']

        for table in tables:
            self.cursor.execute(f"SELECT * FROM {table}")
            self.assertIsNotNone(self.cursor.fetchall())

        for sequence in sequences:
            self.cursor.execute(f"SELECT {sequence}.NEXTVAL FROM DUAL")
            self.assertIsNotNone(self.cursor.fetchall())

    def test_data_insertion(self):
        
        self.cursor.execute("INSERT INTO modules (moduleID, nom, annee) VALUES (1, 'Sample Module', 2022)")
        self.conn.commit()

        self.cursor.execute("SELECT * FROM modules WHERE moduleID = 1")
        result = self.cursor.fetchone()

        # Check if the retrieved data matches the expected values
        self.assertIsNotNone(result)
        self.assertEqual(result[1], 'Sample Module')
        self.assertEqual(result[2], 2022)

#-------------------------------->Performance testing :

import timeit

class TestQcmFunctionsPerformance(unittest.TestCase):

    def test_extract_qcm_performance(self):
        file_path = "sample_qcm.txt"
        execution_time = timeit.timeit(lambda: extract_qcm(file_path), number=10)  # Run 10 times for more accurate measurement
        print(f"Extract QCM Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 5.0)  # Adjust the threshold as needed

    def test_extract_cas_canonic_performance(self):
        file_path = "sample_cas_canonic.txt"
        execution_time = timeit.timeit(lambda: extract_cas_canonic(file_path), number=10)
        print(f"Extract Cas Canonic Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 5.0)

    def test_merge_dataframes_performance(self):
        qcm_df = pd.DataFrame({"cas_number": [1, 2], "question": ["Q1", "Q2"], "choices": [["A", "B", "C", "D"], ["E", "F", "G", "H"]]})
        cas_canonic_df = pd.DataFrame({"cas_number": [1, 2], "cas_canonic": ["Cas1", "Cas2"]})
        execution_time = timeit.timeit(lambda: merge_dataframes(qcm_df, cas_canonic_df), number=10)
        print(f"Merge Dataframes Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 5.0)

    def test_extract_responses_performance(self):
        file_path = "sample_responses.txt"
        execution_time = timeit.timeit(lambda: extract_responses(file_path), number=10)
        print(f"Extract Responses Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 100.0)

    def test_transform_performance(self):
        file_path_qcm = "sample_transform_qcm.txt"
        merged_output_path = "sample_transform_output.csv"
        execution_time = timeit.timeit(lambda: transform(file_path_qcm, merged_output_path), number=1)
        print(f"Transform Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 100.0)

    def test_extract_year_module_performance(self):
        file_path = "sample_files/QCMs/2022/Module1/QCM.txt"
        execution_time = timeit.timeit(lambda: extract_year_module(file_path), number=10)
        print(f"Extract Year Module Performance Time: {execution_time} seconds")
        self.assertLess(execution_time, 5.0)


         
class TestDatabasePerformance(unittest.TestCase):

    def setUp(self):
        # Set up the database connection
        self.dsn_tns = cx_Oracle.makedsn('localhost', '1522', 'xe')
        self.conn = cx_Oracle.connect(user='etl', password='sabrina', dsn=self.dsn_tns)
        self.cursor = self.conn.cursor()

    def tearDown(self):
        # Clean up resources
        self.cursor.close()
        self.conn.close()

    def test_insert_performance(self):
        # Test the performance of data insertion
        start_time = time.time()

        # Insert a large number of records into the 'modules' table
        for i in range(1000):
            self.cursor.execute(f"INSERT INTO modules (moduleID, nom, annee) VALUES ({i}, 'Module-{i}', 2022)")
        
        self.conn.commit()

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"Time taken to insert 1000 records: {execution_time} seconds")

        # Adjust the threshold based on your performance expectations
        threshold_time = 2.0
        self.assertLess(execution_time, threshold_time, f"Insertion took longer than {threshold_time} seconds")

   
  

#------------------------->  Unit BLOB test

# Établir une connexion à la base de données Oracle

# Récupérer le contenu du BLOB
cursor.execute("SELECT pdf_blob FROM COURS WHERE coursID = :file_id", file_id=2)
result = cursor.fetchone()
if result:
    pdf_blob = result[0].read()

    # Enregistrer le contenu du BLOB dans un fichier
    with open('output.pdf', 'wb') as file:
        file.write(pdf_blob)

    print("File saved as 'output.pdf'")
else:
    print("File not found")

# Fermer le curseur et la connexion
cursor.close()
conn.close()



        
#-------------------------> schedule call  
schedule.every().day.at("20:13").do(job)
while True:
    schedule.run_pending()
    time.sleep(1) 



