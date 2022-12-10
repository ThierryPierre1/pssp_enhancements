import dbm
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv #pip install python-dotenv
import os 
from faker import Faker # https://faker.readthedocs.io/en/master/ #pip install Faker
import uuid
import random
load_dotenv()

MYSQL_HOSTNAME = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USERNAME")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}:3306/{MYSQL_DATABASE}'
engine = create_engine(connection_string)

print(engine.table_names())

fake = Faker()

fake_patients = [
    {
        #keep just the first 8 characters of the uuid
        'mrn': str(uuid.uuid4())[:8], 
        'first_name':fake.first_name(), 
        'last_name':fake.last_name(),
        'zip_code':fake.zipcode(),
        'Dob':(fake.date_between(start_date='-90y', end_date='-20y')).strftime("%Y-%m-%d"),
        'gender': fake.random_element(elements=('M', 'F')),
        'phone_number':fake.phone_number()
    } for x in range(50)]

df_fake_patients = pd.DataFrame(fake_patients)
# drop duplicate mrn
df_fake_patients = df_fake_patients.drop_duplicates(subset=['mrn'])

insertQuery = "INSERT INTO patients (mrn, first_name, last_name, zip_code, Dob, gender, phone_number) VALUES (%s, %s, %s, %s, %s, %s, %s)"

for index, row in df_fake_patients.iterrows():
    engine.execute(insertQuery, (row['mrn'], row['first_name'], row['last_name'], row['zip_code'], row['Dob'], row['gender'], row['phone_number']))
    print("Inserting row: ", index)


#####################NDC codes MEDICATIONS ###########################

ndc_codes = pd.read_csv('https://raw.githubusercontent.com/hantswilliams/FDA_NDC_CODES/main/NDC_2022_product.csv')
ndc_codes_1k = ndc_codes.sample(n=1000, random_state=1)
# drop duplicates from ndc_codes_1k
ndc_codes_1k = ndc_codes_1k.drop_duplicates(subset=['PRODUCTNDC'], keep='first')

insertQuery = "INSERT INTO medications (med_ndc, med_human_name) VALUES (%s, %s)"


medRowCount = 0
for index, row in ndc_codes_1k.iterrows():
    medRowCount += 1
    # db_azure.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    engine.execute(insertQuery, (row['PRODUCTNDC'], row['NONPROPRIETARYNAME']))
    print("inserted row: ", index)
    ## stop once we have 50 rows
    if medRowCount == 75:
        break

########################## PATIENT MEDICATION TABLE #############################


df_medications = pd.read_sql_query("SELECT med_ndc, med_human_name FROM medications", engine) 
df_patients = pd.read_sql_query("SELECT mrn FROM patients", engine)


# create a dataframe that is stacked and give each patient a random number of medications between 1 and 5
df_patient_medications = pd.DataFrame(columns=['mrn', 'med_ndc', 'med_human_name'])
# for each patient in df_patient_medications, take a random number of medications between 1 and 10 from df_medications and palce it in df_patient_medications
for index, row in df_patients.iterrows():
    # get a random number of medications between 1 and 5
    numMedications = random.randint(1, 5)
    # get a random sample of medications from df_medications
    df_medications_sample = df_medications.sample(n=numMedications)
    # add the mrn to the df_medications_sample
    df_medications_sample['mrn'] = row['mrn']
    # append the df_medications_sample to df_patient_medications
df_patient_medications = df_patient_medications.append(df_medications_sample)

print(df_patient_medications.head(10))

# now lets add a random medication to each patient
insertQuery = "INSERT INTO patient_medications (mrn, med_ndc, med_human_name) VALUES (%s, %s, %s)"

for index, row in df_patient_medications.iterrows():
    engine.execute(insertQuery, (row['mrn'], row['med_ndc'], row['med_human_name']))
    print("inserted row: ", index)

######################ICD10 codes CONDITIONS ###########################

icd10codes = pd.read_csv('https://raw.githubusercontent.com/Bobrovskiy/ICD-10-CSV/master/2020/diagnosis.csv')
list(icd10codes.columns)
icd10codesShort = icd10codes[['CodeWithSeparator', 'ShortDescription']] #creating two shorter columns
icd10codesShort_1k = icd10codesShort.sample(n=1000)
# drop duplicates
icd10codesShort_1k = icd10codesShort_1k.drop_duplicates(subset=['CodeWithSeparator'], keep='first')


insertQuery = "INSERT INTO conditions (icd10_code, icd10_description) VALUES (%s,%s)"

startingRow = 0
for index, row in icd10codesShort_1k.iterrows():
    startingRow += 1
    print('startingRow: ', startingRow)
    # db_azure.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_azure: ", index)
    engine.execute(insertQuery, (row['CodeWithSeparator'], row['ShortDescription']))
    print("inserted row db_gcp: ", index)
    ## stop once we have 100 rows
    if startingRow == 100: 
        break

# query dbs to see if data is there
df_gcp = pd.read_sql_query("SELECT * FROM conditions", engine)

########################### PATIENT CONDITIONS ###############################

df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", engine)

df_patients = pd.read_sql_query("SELECT mrn FROM patients", engine)

# create a dataframe that is stacked and give each patient a random number of conditions between 1 and 5
df_patient_conditions = pd.DataFrame(columns=['mrn', 'icd10_code'])
# for each patient in df_patient_conditions, take a random number of conditions between 1 and 10 from df_conditions and palce it in df_patient_conditions
for index, row in df_patients.iterrows():
    # get a random number of conditions between 1 and 5
    # numConditions = random.randint(1, 5)
    # get a random sample of conditions from df_conditions
    df_conditions_sample = df_conditions.sample(n=random.randint(1, 5))
    # add the mrn to the df_conditions_sample
    df_conditions_sample['mrn'] = row['mrn']
    # append the df_conditions_sample to df_patient_conditions
    df_patient_conditions = df_patient_conditions.append(df_conditions_sample)
    
print(df_patient_conditions.head(20))

# now lets add a random condition to each patient
insertQuery = "INSERT INTO patient_conditions (mrn, icd10_code) VALUES (%s, %s)"

for index, row in df_patient_conditions.iterrows():
    engine.execute(insertQuery, (row['mrn'], row['icd10_code']))
    print("inserted row: ", index)

df_conditions = pd.read_sql_query("SELECT icd10_code FROM conditions", engine)
df_patients = pd.read_sql_query("SELECT mrn FROM patients", engine)