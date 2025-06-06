import asyncio
import uuid
import json
from datetime import datetime

from connection import Connection
from pymarc import Field, MARCReader, Record, Subfield
FILE_NAME = 'eafit_4.mrc'
INSTANCE_RELATIONSHIP_TYPE_ID = '30773a27-b485-4dab-aeb6-b8c04fa3cb17'
OUTPUT_FILE =  f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{FILE_NAME.split(".")[0]}_out.json"

def getUUID():
    return str(uuid.uuid4())

def getSubInstanceId(record = Record):
    return record.get_fields('999')[0]['i'] if record.get_fields('999') else None

def getSuperInstanceId(record):
    fields773 = record.get_fields('773')
    for field in fields773:
        for subfield in field.subfields:
            print(subfield)
            if subfield.code == 'w' :
                return  subfield.value
    return ''


def setLine():
    return None

def writeLine(data, file_handle):
    json.dump(data, file_handle)
    file_handle.write('\n')

# Ejemplo de uso
if __name__ == "__main__":
    async def main():
        conn = Connection()
        token = await conn.get_token()
        print(f"✅ Token: {token}")
        with open(FILE_NAME, 'rb') as fh, open(OUTPUT_FILE, 'w', encoding='utf-8') as out:
            reader = MARCReader(fh)
            for record in reader:
                id = getUUID()
                subInstanceId = getSubInstanceId(record)
                superInstanceId = getSuperInstanceId(record)

                if not subInstanceId or not superInstanceId:
                    continue  # omitir si falta alguno

                data = {
                    "id": id,
                    "subInstanceId": subInstanceId,
                    "superInstanceId": superInstanceId,
                    "instanceRelationshipTypeId": INSTANCE_RELATIONSHIP_TYPE_ID
                }
                writeLine(data, out)
                print("✅ Escrito:", data)

    asyncio.run(main())