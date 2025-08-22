import streamlit as st
import pandas as pd
from io import StringIO
from contextlib import redirect_stdout
import sys
from databricks.connect import DatabricksSession

# Optional: Setup PySpark
#from pyspark.sql import SparkSession



if "spark" not in st.session_state:
    st.write("Starting App....")
    try:
        # st.session_state.spark = SparkSession.builder \
        # .appName("Streamlit Notebook") \
        # .master("local[*]") \
        # .getOrCreate()
        st.session_state.spark= DatabricksSession.builder.remote(serverless=True).getOrCreate()
    except Exception as e:
        st.error(e)
        st.session_state.spark=None
        
                
spark = st.session_state.spark
if spark:
    st.title("📓 Streamlit Jupyter-like Notebook")

    # ------------------------
    # Manage cells in session state
    # ------------------------
    if "cells" not in st.session_state:
        st.session_state.cells = [""]
    if "outputs" not in st.session_state:
        st.session_state.outputs = [None]

    # ------------------------
    # Show existing cells
    # ------------------------
    for i, code in enumerate(st.session_state.cells):
        st.markdown(f"### 📝 Cell {i+1}")
        code_input = st.text_area(f"Code {i+1}", value=code, height=100, key=f"code_input_{i}")
        st.session_state.cells[i] = code_input

        if st.button(f"Run Cell {i+1}", key=f"run_btn_{i}"):
            # Redirect stdout to capture prints
            output_buffer = StringIO()
            local_vars = st.session_state.get("kernel_vars", {})
            try:
                with redirect_stdout(output_buffer):
                    # Evaluate the code in shared local_vars dict
                    exec(code_input, globals(), local_vars)
                result_output = output_buffer.getvalue()
                st.session_state.outputs[i] = (result_output, None)
            except Exception as e:
                st.session_state.outputs[i] = ("", f"🚨 {str(e)}")
            st.session_state.kernel_vars = local_vars

        # Show outputs
        if st.session_state.outputs[i]:
            out_text, out_error = st.session_state.outputs[i]
            if out_text:
                st.code(out_text)
            if out_error:
                st.error(out_error)

        st.divider()

    # ------------------------
    # Add new cell button
    # ------------------------
    if st.button("➕ Add New Cell"):
        st.session_state.cells.append("")
        st.session_state.outputs.append(None)


if st.button("show env variables"):
        st.write("printing app variables")
        st.write(f"{os.environ}")