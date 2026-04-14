"""Read and print the contents of the LGTM instruction PDF."""
import pdfplumber

pdf_path = r"C:\Users\adamc\PycharmProjects\windfarmdata\instructions\Uruchomienie i połączenie stacku LGTM z aplikacją FastAPI-2.pdf"

with pdfplumber.open(pdf_path) as pdf:
    for i, page in enumerate(pdf.pages):
        print(f"\n--- PAGE {i+1} ---")
        text = page.extract_text()
        if text:
            print(text)

