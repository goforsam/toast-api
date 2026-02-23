"""Single source of truth for all Toast ETL client configurations.

To add a new client:
  1. Add an entry to CLIENTS below.
  2. Add a one-line case to deploy_all.sh (PREFIX only).
  3. Ensure credentials exist in Secret Manager:
       TOAST_CLIENT_ID{secret_suffix} and TOAST_CLIENT_SECRET{secret_suffix}
"""

CLIENTS = {
    "purpose": {
        "secret_suffix": "",           # Secrets: TOAST_CLIENT_ID, TOAST_CLIENT_SECRET
        "bq_dataset_id": "purpose",
        "url_prefix": "toast",         # Function names: toast-orders-etl, etc.
        "restaurant_guids": [
            "6d035dad-924f-47b4-ba93-fd86575e73a3",
            "53ae28f1-87c7-4a07-9a43-b619c009b7b0",
            "def5e222-f458-41d0-bff9-48abaf20666a",
            "42f246b1-82f1-4048-93c1-d63554c7d9ef",
            "a405942e-179b-4f3f-a75b-a0d18882bd7f",
            "d587bfe9-9faa-48a8-9938-1a23ad36bc9e",
            "da6f0893-d17c-4f93-b7ee-0c708d2611a9",
            "a6a87c64-734e-4f39-90dc-598b5e743105",
            "e629b6e6-85f5-466f-9427-cfbb4f2a6bfe",
            "290ca643-8ee4-4d8f-9c70-3793e15ae8a6",
            "eaa7b168-db38-45be-82e8-bd25e6647fd1",
            "a4b4a7a2-0309-4451-8b62-ca0c98858a84",
            "d44d5122-3412-459a-946d-f91a5da03ea3",
        ],
    },
    "rodrigos": {
        "secret_suffix": "_RODRIGOS",  # Secrets: TOAST_CLIENT_ID_RODRIGOS, etc.
        "bq_dataset_id": "rodrigos",
        "url_prefix": "rodrigos",
        "restaurant_guids": [
            "ab3c4f80-5529-4b5f-bba1-cc9abaf33431",
            "3383074f-b565-4501-ae86-41f21c866cba",
            "8cb95c1f-2f82-4f20-9dce-446a956fd4bb",
            "bef05e5c-3b38-49f3-9b8d-ca379130f718",
            "8c37412b-a13b-4edd-bbd8-b26222fcbe68",
            "dedecf4f-ee34-41ab-a740-f3b461eed4eb",
            "eea6e77a-46b2-4631-907e-10d85a845bb8",
            "e2fbc555-2cc4-49ee-bbdc-1e4c652ec6f4",
            "d0bbc362-63d4-4277-af85-2bf2c808bdc7",
            "1903fd30-c0ff-4682-b9af-b184c77d9653",
        ],
    },
    "slim_husky": {
        "secret_suffix": "_SLIM",      # Secrets: TOAST_CLIENT_ID_SLIM, etc.
        "bq_dataset_id": "slim_husky",
        "url_prefix": "slim",
        "restaurant_guids": [
            "9ee73d8b-7d6d-4227-b005-9a3e6e749dbe",  # Atlanta/Metropolitan
            "cd8c8f17-7868-4281-97a1-589c0b0799e4",  # Memphis/Downtown
            "89674e99-65bb-4855-998c-c6eee25fe032",  # Nashville/Antioch
            "c50c9ccc-7cb9-42e9-8359-04414258eb6a",  # Nashville/Buchanan Arts District
            "dfcda609-9262-4181-9b26-a9db7a87c2ea",  # Nashville/5th + Broadway
            "2fe1af2a-1021-4b80-b060-4b70fad83e9b",  # Franklin
            "b00be8e0-a7d9-4a90-a4e2-3d8191a86796",  # Murfreesboro/MTSU
            "6371f5c4-a26b-49ba-943a-c27178a21dad",  # Nashville/Belmont Univ
        ],
    },
}
