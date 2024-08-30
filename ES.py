from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

# Create the client instance

#client = Elasticsearch(
#    "https://192.168.1.11:9200",
#    ca_certs = os.getenv('PATH_TO_REMOTE_CERTS'),
#    api_key = os.getenv('REMOTE_DB_API_KEY'),
#)

client = Elasticsearch(
    "https://localhost:9200",
    ca_certs = os.getenv('PATH_TO_LOCAL_CERTS'),
    api_key = os.getenv('LOCAL_DB_API_KEY'),
)

# Successful response!
print(client.info())

if client.indices.exists("rbc_top_news_index"):
    client.indices.delete("rbc_top_news_index")

if client.indices.exists("rbc_top_news_history_index"):
    client.indices.delete("rbc_top_news_history_index")

setup = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1 
    },
    "mappings": {
        "properties": {
            "header": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                    }
                }
            },
            "subheader": {"type": "text"},
            "pub_date": {"type": "date"},
            "modif_date": {"type": "date"},
            "overview": {"type": "text"},
            "picture": {"type": "keyword"},
            "article_text": {"type": "text"},
        }
    }
}

client.indices.create(index="rbc_top_news_index", body=setup)

history_setup = {
    "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1 
    },
    "mappings": {
        "properties": {
            "history": {
                "properties": {
                    "picture": {"type": "keyword"},
                    "header": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "subheader": {"type": "text"},
                    "overview": {"type": "text"},
                    "article_text": {"type": "text"},
                    "modif_date": {"type": "date"}
                }
            }
        }
    }
}

client.indices.create(index="rbc_top_news_history_index", body=history_setup)
