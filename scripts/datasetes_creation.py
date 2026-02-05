from google.cloud import bigquery

def create_dataset_and_tables(project_id, dataset_id, schema):
    client = bigquery.Client(project=project_id)

    # Check if dataset exists
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        # Create dataset if it does not exist
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your location
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created.")

    # Check and create tables according to the schema
    for table_name, table_schema in schema.items():
        table_ref = dataset_ref.table(table_name)
        try:
            client.get_table(table_ref)
            print(f"Table {table_name} already exists in dataset {dataset_id}.")
        except Exception:
            # Create table if it does not exist
            table = bigquery.Table(table_ref, schema=table_schema)
            client.create_table(table)
            print(f"Table {table_name} created in dataset {dataset_id}.")

if __name__ == "__main__":
    project_id = "your_project_id"
    dataset_id = "your_dataset_id"
    schema = {
        "your_table_name": [
            bigquery.SchemaField("field_name", "STRING"),
            bigquery.SchemaField("field_age", "INTEGER"),
            # Add more fields as needed
        ],
    }

    create_dataset_and_tables(project_id, dataset_id, schema)