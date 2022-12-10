from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, abort
import pandas as pd 
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

mysql_hostname = os.getenv("MYSQL_HOST")
mysql_username = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")

connection_string = f'mysql+pymysql://{mysql_username}:{mysql_password}@{mysql_hostname}:3306/{mysql_database}'
engine = create_engine(connection_string)

tableNames_gcp = engine.table_names()

print(engine.table_names())

table_prod_patients = """
create table if not exists patients (
id int auto_increment,
mrn varchar(255) default null unique,
first_name varchar(255) default null,
last_name varchar(255) default null,
zip_code varchar(255) default null,
dob varchar(255) default null,
gender varchar(255) default null,
phone_number varchar(255) default null,
PRIMARY KEY (id)); """


table_prod_medications = """
create table if not exists medications(
id int auto_increment,
med_ndc varchar(255) default null unique,
med_human_name varchar(255) default null,
med_is_dangerous varchar(255) default null,
PRIMARY KEY (id)); """

table_prod_conditions = """
create table if not exists conditions(
id int auto_increment,
icd10_code varchar(255) default null unique,
icd10_description varchar(255) default null,
PRIMARY KEY (id) ); """

table_prod_patients_medications = """
create table if not exists patient_medications(
id int auto_increment,
mrn varchar(255) default null,
med_ndc varchar(255) default null,
med_human_name varchar(255) default null unique,
PRIMARY KEY (id),
FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
FOREIGN KEY (med_ndc) REFERENCES medications(med_ndc) ON DELETE CASCADE); """


table_prod_patient_conditions = """
create table if not exists patient_conditions (
    id int auto_increment,
    mrn varchar(255) default null,
    icd10_code varchar(255) default null,
    PRIMARY KEY (id),
    FOREIGN KEY (mrn) REFERENCES patients(mrn) ON DELETE CASCADE,
    FOREIGN KEY (icd10_code) REFERENCES conditions(icd10_code) ON DELETE CASCADE); """

engine.execute(table_prod_patients)
engine.execute(table_prod_medications)
engine.execute(table_prod_conditions)
engine.execute(table_prod_patients_medications)
engine.execute(table_prod_patient_conditions)

gcp_tables = engine.table_names()