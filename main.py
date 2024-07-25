import csv
import psycopg2
from datetime import datetime

# Database connection parameters
db_params = {
    'dbname': 'assetscan_qa',
    'user': 'postgres',
    'password': 'W3lc0m3@B3nt0',
    'host': '10.100.2.8',
    'port': '5432'
}

# Path to the CSV file
csv_file_path = 'update_rera.csv'

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Create the INSERT INTO SQL statement
insert_sql = """
INSERT INTO rera_master (
    id, project_name, promoter_name, last_modified_date, organization_type, past_experience, other_state_registration, other_type_organization, any_criminal_record, gst_number, authority_name, approved_date, proposed_completion_date, litigation, promoter_telengana_rera_order, approval_number, project_status, project_type, survey_no, total_area_in_sqmts, net_area_in_sqmt, proposed_building_units, total_building_unit, approved_build_up_area_in_sqmt, mortgage_area_in_sqmt, project_state, project_district, project_mandal, project_village, project_street, project_locality, project_pincode, tower_name, no_of_slabs, no_of_floors, total_parking_area_in_sqmt
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

def convert_date(date_str):
    if date_str and isinstance(date_str, str):
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None

def safe_numeric(value, dtype='float'):
    if value == '' or value is None:
        return None
    try:
        if dtype == 'int':
            return int(value)
        elif dtype == 'float':
            return float(value)
    except (ValueError, TypeError):
        return None

def process_row(row):
    return (
        safe_numeric(row.get('id'), 'int'),
        row.get('project_name'),
        row.get('promoter_name'),
        convert_date(row.get('last_modified_date')),
        row.get('organization_type'),
        row.get('past_experience'),
        row.get('other_state_registration'),
        row.get('other_type_organization'),
        row.get('any_criminal_record'),
        row.get('gst_number'),
        row.get('authority_name'),
        convert_date(row.get('approved_date')),
        convert_date(row.get('proposed_completion_date')),
        row.get('litigation'),
        row.get('promoter_telengana_rera_order'),
        row.get('approval_number'),
        row.get('project_status'),
        row.get('project_type'),
        row.get('survey_no'),
        safe_numeric(row.get('total_area_in_sqmts'), 'float'),
        safe_numeric(row.get('net_area_in_sqmt'), 'float'),
        row.get('proposed_building_units'),
        safe_numeric(row.get('total_building_unit'), 'float'),
        safe_numeric(row.get('approved_build_up_area_in_sqmt'), 'float'),
        safe_numeric(row.get('mortgage_area_in_sqmt'), 'float'),
        row.get('project_state'),
        row.get('project_district'),
        row.get('project_mandal'),
        row.get('project_village'),
        row.get('project_street'),
        row.get('project_locality'),
        row.get('project_pincode'),
        row.get('tower_name'),
        safe_numeric(row.get('no_of_slabs'), 'float'),
        safe_numeric(row.get('no_of_floors'), 'int'),
        safe_numeric(row.get('total_parking_area_in_sqmt'), 'float')
    )

with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    error_count = 0

    for row in reader:
        try:
            processed_row = process_row(row)
            cursor.execute(insert_sql, processed_row)
        except psycopg2.Error as e:
            print(f"Error inserting row: {e.pgcode} - {e.pgerror}")
            conn.rollback()
            error_count += 1

# Commit the transaction and close the connection
conn.commit()
cursor.close()
conn.close()

print(f"Data insertion completed with {error_count} errors.")
