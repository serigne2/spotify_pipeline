from extract import extract_data
from transform import transform
from load import load

def pipeline():
    extracted_data = extract_data()
    transformed_data = transform()
    load()

if __name__ == "__main__":
    pipeline()
