
# COMMAND ----------

# MAGIC %pip install --upgrade databricks-langchain langchain-community langchain databricks-sql-connector

# COMMAND ----------

from langchain.chains import RetrievalQA
import mlflow
# COMMAND ----------

from databricks_langchain import ChatDatabricks

chat_model = ChatDatabricks(
    endpoint="databricks-meta-llama-3-1-405b-instruct",
    temperature=0.1,
    max_tokens=4096,
)

# COMMAND ----------

from databricks_langchain import DatabricksVectorSearch

vector_store = DatabricksVectorSearch(index_name="workspace.llm_rag.databricks_vector_index",columns=["url","content"],)
retriever = vector_store.as_retriever(search_kwargs={"k": 5})
response=retriever.invoke("provide step-by-step process to do disaster recovery in  Databricks")

from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate




system_prompt = (
    "Use the given context to answer the question. "
    "If you don't know the answer, say you don't know. "
    "Use three sentence maximum and keep the answer concise. "
    "Context: {context}"
)
prompt = ChatPromptTemplate.from_messages(
    [
        ("system", system_prompt),
        ("human", "{input}"),
    ]
)
question_answer_chain = create_stuff_documents_chain(chat_model, prompt)
retrievalbot = create_retrieval_chain(retriever, question_answer_chain)
mlflow.models.set_model(retrievalbot) 
