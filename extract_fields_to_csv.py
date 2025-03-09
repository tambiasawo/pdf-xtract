import PyPDF2
import csv
import os

# File paths.
PDF_FILE = "rent_agreement.pdf"    # Your fillable PDF file
CSV_FILE = "tenant_data.csv"         # CSV file to store the extracted data

def extract_form_fields(pdf_path):
    """
    Extracts AcroForm field data from the PDF using PyPDF2.
    Returns a dictionary of field names and their information.
    """
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        fields = reader.get_fields()  # Returns a dictionary: { field_name: field_info, ... }
    return fields

def save_fields_to_csv(fields, csv_file):
    """
    Maps the extracted form fields to a standardized CSV header and writes the data to CSV.
    
    Expected CSV header:
    Landlord 1 First Name, Landlord 1 Last Name, Landlord 2 First Name, Landlord 2 Last Name,
    Tenant 1 First Name, Tenant 1 Last Name, Tenant 2 First Name, Tenant 2 Last Name,
    Phone Number, Address, Lease Start Date, Lease Expiry Date, Monthly Rent
    """
    # Define the CSV header.
    headers = ["Landlord 1 First Name", "Landlord 1 Last Name",
               "Landlord 2 First Name", "Landlord 2 Last Name",
               "Tenant 1 First Name", "Tenant 1 Last Name",
               "Tenant 2 First Name", "Tenant 2 Last Name",
               "Phone Number", "Address", "Lease Start Date", "Lease Expiry Date", "Monthly Rent"]
    
    # Initialize our output dictionary with empty strings.
    output_data = {key: "" for key in headers}
    
    # Define a mapping from PDF form field names (lowercase) to our CSV header keys.
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
        # Additional fields (Address, Lease dates, Monthly Rent) will be handled separately.
    }
    
    # Map individual fields from the extracted data.
    for field_name, field_info in fields.items():
        key = field_name.strip().lower()
        if key in mapping:
            value = field_info.get("/V")
            if value:
                output_data[mapping[key]] = str(value).strip()
    
    # Build the Address field by concatenating several PDF fields, if available.
    address_parts = []
    for fname in ["unit #", "street number and street name1", "City1", "Province1", "Postalcode1"]:
        key = fname.strip().lower()
        if key in fields:
            val = fields[key].get("/V")
            if val:
                address_parts.append(str(val).strip())
    if address_parts:
        output_data["Address"] = ", ".join(address_parts)
    
    # Build the Lease Start Date field.
    start_parts = []
    # Assuming the PDF contains fields like "day", "month1", and "year1" for start date.
    for fname in ["day", "month1", "year1"]:
        key = fname.strip().lower()
        if key in fields:
            val = fields[key].get("/V")
            if val:
                start_parts.append(str(val).strip())
    if start_parts:
        output_data["Lease Start Date"] = " ".join(start_parts)
    
    # Build the Lease Expiry Date field.
    end_parts = []
    # Assuming the PDF contains fields like "day", "month2", and "year2" for expiry date.
    for fname in ["day", "month2", "year2"]:
        key = fname.strip().lower()
        if key in fields:
            val = fields[key].get("/V")
            if val:
                end_parts.append(str(val).strip())
    if end_parts:
        output_data["Lease Expiry Date"] = " ".join(end_parts)
    
    # For Monthly Rent, try to get the value from a field like "the tenant will pay the rent of"
    rent_field = "the tenant will pay the rent of"
    if rent_field in fields:
        val = fields[rent_field].get("/V")
        if val:
            output_data["Monthly Rent"] = str(val).strip()
    
    # Write the output_data dictionary as a row to the CSV file.
    write_header = not os.path.isfile(csv_file)
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if write_header:
            writer.writeheader()
        writer.writerow(output_data)
    
    print(f"Structured data saved to {csv_file}")

def main():
    # Extract the form fields from the PDF.
    fields = extract_form_fields(PDF_FILE)
    
    if not fields:
        print("No form fields found in the PDF.")
        return
    
    # Save the mapped fields into a CSV file.
    save_fields_to_csv(fields, CSV_FILE)

if __name__ == "__main__":
    main()
