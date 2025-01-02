from dotenv import load_dotenv
from google.cloud import bigquery

# Set the path to your credentials file
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\faraz\Desktop\coding\DataPipelineProject\credential.json"
load_dotenv()

# Initialize the BigQuery client
client = bigquery.Client()

# Test the connection
print("Connected to BigQuery!")
