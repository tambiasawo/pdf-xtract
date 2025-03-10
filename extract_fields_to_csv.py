import boto3
import csv
import io
import PyPDF2

s3 = boto3.client("s3")

# Bucket names and key for output CSV.
INPUT_BUCKET = "tenants-lease-agreements"
OUTPUT_BUCKET = "rent-reporting-csv"
OUTPUT_CSV_KEY = "tenant_data.csv"

def extract_form_fields_from_bytes(pdf_bytes):
    """Extracts AcroForm field data from PDF bytes using PyPDF2."""
    file_obj = io.BytesIO(pdf_bytes)
    reader = PyPDF2.PdfReader(file_obj)
    return reader.get_fields()

def build_csv_row_from_fields(fields):
    """
    Maps the PDF field data (as returned by PyPDF2) to our desired CSV row.
    It combines the lease start (day + month1 + year1) and lease expiry dates,
    and builds an Address from unit, street, City1, Province1, and Postalcode1.
    """
    headers = ["Landlord 1 First Name", "Landlord 1 Last Name",
               "Landlord 2 First Name", "Landlord 2 Last Name",
               "Tenant 1 First Name", "Tenant 1 Last Name",
               "Tenant 2 First Name", "Tenant 2 Last Name",
               "Phone Number", "Address", "Lease Start Date", "Lease Expiry Date", "Monthly Rent"]
    output_data = {key: "" for key in headers}
    
    # Mapping simple fields.
    mapping = {
        "last name": "Landlord 1 Last Name",
        "first and middle names": "Landlord 1 First Name",
        "last name_2": "Landlord 2 Last Name",
        "first and middle names_2": "Landlord 2 First Name",
        "last name_3": "Tenant 1 Last Name",
        "first and middle names_3": "Tenant 1 First Name",
        "last name_4": "Tenant 2 Last Name",
        "first and middle names_4": "Tenant 2 First Name",
        "undefined_2": "Phone Number"
    }
    
    # Populate the simple fields via mapping
    for field_name, field_info in fields.items():
        key_lower = field_name.strip().lower()
        for pdf_key, csv_key in mapping.items():
            if pdf_key in key_lower:
                value = field_info.get("/V")
                if value:
                    output_data[csv_key] = str(value).strip()
                break

    # Build Address field (unit #, street name, city, province, postal code)
    address_parts = []
    for fname in ["unit #", "street number and street name1", "City1", "Province1", "Postalcode1"]:
        for field_name in fields.keys():
            if fname.lower() in field_name.lower():
                val = fields[field_name].get("/V")
                if val:
                    address_parts.append(str(val).strip())
                break
    if address_parts:
        output_data["Address"] = ", ".join(address_parts)
    
    # Lease Start Date: day from "this tenancy created by this agreement starts on"
    start_day = ""
    for field_name in fields.keys():
        if "this tenancy created by this agreement starts on" in field_name.lower():
            val = fields[field_name].get("/V")
            if val:
                start_day = str(val).strip()
            break
    month1 = str(fields.get("month1", {}).get("/V", "")).strip()
    year1  = str(fields.get("year1",  {}).get("/V", "")).strip()
    if start_day or month1 or year1:
        output_data["Lease Start Date"] = f"{start_day} {month1} {year1}".strip()
    
    # Lease Expiry Date: day from "this tenancy created by this agreement ends on"
    end_day = ""
    for field_name in fields.keys():
        if "this tenancy created by this agreement ends on" in field_name.lower():
            val = fields[field_name].get("/V")
            if val:
                end_day = str(val).strip()
            break
    month2 = str(fields.get("month2", {}).get("/V", "")).strip()
    year2  = str(fields.get("year2",  {}).get("/V", "")).strip()
    if end_day or month2 or year2:
        output_data["Lease Expiry Date"] = f"{end_day} {month2} {year2}".strip()
    
    # Monthly Rent: from field containing "the tenant will pay the rent of"
    rent_field_substring = "the tenant will pay the rent of"
    for field_name in fields.keys():
        if rent_field_substring in field_name.lower():
            val = fields[field_name].get("/V")
            if val:
                output_data["Monthly Rent"] = str(val).strip()
            break
    
    return output_data

def append_csv_row_to_s3(row, bucket, key):
    """
    Reads the existing CSV file from S3 (if exists), appends a new row, and writes it back.
    If the file does not exist, creates a new CSV file with header.
    """
    headers = ["Landlord 1 First Name", "Landlord 1 Last Name",
               "Landlord 2 First Name", "Landlord 2 Last Name",
               "Tenant 1 First Name", "Tenant 1 Last Name",
               "Tenant 2 First Name", "Tenant 2 Last Name",
               "Phone Number", "Address", "Lease Start Date", "Lease Expiry Date", "Monthly Rent"]
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        existing_csv = csv_content
    except s3.exceptions.NoSuchKey:
        existing_csv = ""
    except Exception as e:
        print(f"Error reading existing CSV from S3: {e}")
        existing_csv = ""
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    if not existing_csv:
        writer.writeheader()
    else:
        # preserve existing CSV content, then add a newline
        output.write(existing_csv.rstrip("\n") + "\n")
    writer.writerow(row)
    new_csv = output.getvalue()
    
    s3.put_object(Bucket=bucket, Key=key, Body=new_csv.encode('utf-8'))
    print(f"CSV updated and uploaded to s3://{bucket}/{key}")

def lambda_handler(event, context):
    """
    AWS Lambda handler triggered by an S3 event when a PDF is uploaded into the tenants-lease-agreements bucket.
    It reads the PDF, extracts the AcroForm fields using PyPDF2, maps them into our standardized CSV format,
    and appends the row to a CSV file in the rent-reporting-csv bucket.
    """
    record = event['Records'][0]
    src_bucket = record['s3']['bucket']['name']
    src_key = record['s3']['object']['key']
    print(f"Processing file s3://{src_bucket}/{src_key}")
    
    # Download the PDF file from S3
    response = s3.get_object(Bucket=src_bucket, Key=src_key)
    pdf_bytes = response['Body'].read()
    
    # Extract form fields
    fields = extract_form_fields_from_bytes(pdf_bytes)
    if not fields:
        print("No form fields found in the PDF.")
        return {"statusCode": 400, "body": "No form fields found in the PDF."}
    
    # Build the CSV row
    csv_row = build_csv_row_from_fields(fields)
    
    # Append row to CSV in S3
    append_csv_row_to_s3(csv_row, OUTPUT_BUCKET, OUTPUT_CSV_KEY)
    
    return {"statusCode": 200, "body": "PDF processed and CSV updated."}
