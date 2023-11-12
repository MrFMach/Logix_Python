import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime

class DatabaseHandler:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()

    def create_table(self, table_name, columns):
        self.cursor.execute(f'DROP TABLE IF EXISTS {table_name}')
        self.cursor.execute(f'CREATE TABLE IF NOT EXISTS {table_name} ({columns})')

    def commit_and_close(self):
        self.conn.commit()
        self.conn.close()

class DataInserter:
    def __init__(self, db_handler, root):
        self.db_handler = db_handler
        self.root = root

    def insert_data(self, table_name, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join([f':{col}' for col in data.keys()])
        query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        self.db_handler.cursor.execute(query, data)

def main():
    # Nome do arquivo XML
    nome_arquivo = 'Alarms.xml'

    # Parse do arquivo XML
    tree = ET.parse(nome_arquivo)
    root = tree.getroot()

    # Criar instância do manipulador de banco de dados
    db_handler = DatabaseHandler('alarms.db')

    # Definir colunas para a tabela 'triggers'
    triggers_columns = '''
        id TEXT PRIMARY KEY,
        type TEXT,
        exp TEXT,
        label TEXT
    '''

    # Criar tabela 'triggers'
    db_handler.create_table('triggers', triggers_columns)

    # Criar instância do inseridor de dados
    data_inserter_triggers = DataInserter(db_handler, root)

    # Inserir dados na tabela 'triggers'
    for trigger in root.findall(".//trigger"):
        trigger_data = {
            'id': trigger.get('id'),
            'type': trigger.get('type'),
            'exp': trigger.get('exp'),
            'label': trigger.get('label')
        }
        data_inserter_triggers.insert_data('triggers', trigger_data)

    # Definir colunas para a tabela 'messages'
    messages_columns = '''
        id TEXT PRIMARY KEY,
        trigger_value TEXT,
        identifier TEXT,
        trigger TEXT,
        text TEXT
    '''

    # Criar tabela 'messages'
    db_handler.create_table('messages', messages_columns)

    # Criar instância do inseridor de dados
    data_inserter_messages = DataInserter(db_handler, root)

    # Inserir dados na tabela 'messages'
    for message in root.findall(".//message"):
        message_data = {
            'id': message.get('id'),
            'trigger_value': message.get('trigger-value'),
            'identifier': message.get('identifier'),
            'trigger': message.get('trigger'),
            'text': message.get('text')
        }
        data_inserter_messages.insert_data('messages', message_data)

    # Criar tabela 'tags'
    tags_columns = '''
        tag TEXT PRIMARY KEY,
        exp TEXT,
        identifier TEXT,
        text TEXT
    '''

    db_handler.create_table('tags', tags_columns)

    # Inserir dados na tabela 'tags' com relação entre 'triggers' e 'messages'
    for trigger in root.findall(".//trigger"):
        trigger_id = trigger.get('id')
        message_id = f'M{trigger_id[1:]}'
        trigger_exp = trigger.get('exp')
        message_text = root.find(f".//message[@id='{message_id}']").get('text')
        message_identifier = root.find(f".//message[@id='{message_id}']").get('identifier')

        # Corrigir identificador ('identifier') removendo a letra 'T'
        trigger_identifier = trigger_id[1:]

        tag_data = {
            'tag': trigger_exp[trigger_exp.find(']') + 1:trigger_exp.find('}')],
            'exp': trigger_exp,
            'identifier': message_identifier,
            'text': message_text
        }

        data_inserter_tags = DataInserter(db_handler, root)
        data_inserter_tags.insert_data('tags', tag_data)

    # Criar tabela 'events'
    events_columns = '''
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        datetime TEXT,
        event TEXT
    '''

    db_handler.create_table('events', events_columns)

    # Inserir dados na tabela 'events'
    current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    event_data = {
        'datetime': current_datetime,
        'event': 'Exemplo de evento'
    }
    data_inserter_events = DataInserter(db_handler, root)
    data_inserter_events.insert_data('events', event_data)

    # Commit e fechar a conexão
    db_handler.commit_and_close()

    # Imprimir o conteúdo da tabela 'tags'
    db_handler.conn = sqlite3.connect('alarms.db')
    db_handler.cursor = db_handler.conn.cursor()

    print("\nConteúdo da tabela 'tags':")
    db_handler.cursor.execute('SELECT * FROM tags')
    for row in db_handler.cursor.fetchall():
        print(row)

    # Imprimir o conteúdo da tabela 'events'
    print("\nConteúdo da tabela 'events':")
    db_handler.cursor.execute('SELECT * FROM events')
    for row in db_handler.cursor.fetchall():
        print(row)

    # Fechar a conexão
    db_handler.commit_and_close()

if __name__ == "__main__":
    main()
