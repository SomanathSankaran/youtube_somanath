# Databricks notebook source
from pydantic import BaseModel, Field
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, BooleanType,
    ArrayType, MapType, DataType, LongType
)
from typing import Type, Optional, List, Dict, Any, Union


# Mapping PySpark DataTypes to Python Types
SPARK_TO_PYTHON = {
    IntegerType: int,
    LongType: int,
    StringType: str,
    DoubleType: float,
    BooleanType: bool,
}


def spark_type_to_python(data_type: DataType) -> Any:
    """Convert a PySpark DataType to a corresponding Python type."""
    if isinstance(data_type, StructType):
        return spark_schema_to_pydantic("NestedModel", data_type)
    elif isinstance(data_type, ArrayType):
        return List[spark_type_to_python(data_type.elementType)]
    elif isinstance(data_type, MapType):
        return Dict[str, spark_type_to_python(data_type.valueType)]
    return SPARK_TO_PYTHON.get(type(data_type), Any)


def spark_schema_to_pydantic(name: str, schema: StructType) -> Type[BaseModel]:
    
    """Convert a PySpark StructType schema to a Pydantic model dynamically."""
    annotations = {}
    defaults = {}

    for field in schema.fields:
        python_type = spark_type_to_python(field.dataType)
        
        # If field is nullable, wrap it in Optional[]
        if field.nullable:
            python_type = Optional[python_type]

        # Add type annotations correctly
        annotations[field.name] = python_type
        defaults[field.name] = Field(None, description=f"Auto-generated from Spark: {field.dataType.simpleString()}")
        #print(defaults)

    # Dynamically create a new Pydantic model
    return type(name, (BaseModel,), {"__annotations__": annotations, **defaults})



# COMMAND ----------

import json
from pydantic import BaseModel, Field
from typing import Type, Optional, List, Dict, Any, Union


# Mapping JSON Schema Types to Python Types
JSON_TO_PYTHON = {
    "integer": int,
    "long": int,
    "string": str,
    "double": float,
    "boolean": bool,
}


def json_schema_to_pydantic(name: str, schema: dict) -> Type[BaseModel]:
    """
    Convert a JSON schema (in the given PySpark-like format) to a Pydantic model dynamically.
    """
    annotations = {}
    defaults = {}

    for field in schema["fields"]:
        field_name = field["name"]
        field_type = field["type"]
        nullable = field["nullable"]

        python_type = parse_json_type(field_type)

        # Wrap in Optional if nullable
        if nullable:
            python_type = Optional[python_type]

        # Add type annotations correctly
        annotations[field_name] = python_type
        defaults[field_name] = Field(
            None, description=f"Auto-generated from JSON Schema: {field_type}"
        )
        #print(defaults)

    # Dynamically create a new Pydantic model
    return type(name, (BaseModel,), {"__annotations__": annotations, **defaults})


def parse_json_type(json_type: Union[str, dict]) -> Any:
    """
    Recursively parse JSON schema types and return the corresponding Python type.
    """
    if isinstance(json_type, str):
        return JSON_TO_PYTHON.get(json_type, Any)

    elif isinstance(json_type, dict):
        if json_type["type"] == "array":
            return List[parse_json_type(json_type["elementType"])]

        elif json_type["type"] == "struct":
            return json_schema_to_pydantic("NestedModel", json_type)

    return Any






# COMMAND ----------

import json
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, BooleanType,
    ArrayType, LongType, DataType
)

# Mapping JSON Schema types to PySpark DataTypes
JSON_TO_SPARK = {
    "integer": IntegerType(),
    "long": LongType(),
    "string": StringType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
}


def json_schema_to_spark(schema: dict) -> StructType:
    """
    Convert a JSON schema (in PySpark-like format) to a PySpark StructType.
    """
    fields = []

    for field in schema["fields"]:
        field_name = field["name"]
        field_type = field["type"]
        nullable = field["nullable"]

        spark_type = parse_json_type(field_type)

        # Create StructField
        fields.append(StructField(field_name, spark_type, nullable))

    return StructType(fields)


def parse_json_type(json_type) -> DataType:
    """
    Recursively parse JSON schema types and return the corresponding PySpark DataType.
    """
    if isinstance(json_type, str):
        return JSON_TO_SPARK.get(json_type, StringType())  # Default to StringType

    elif isinstance(json_type, dict):
        if json_type["type"] == "array":
            element_type = parse_json_type(json_type["elementType"])
            return ArrayType(element_type, json_type.get("containsNull", True))

        elif json_type["type"] == "struct":
            return json_schema_to_spark(json_type)

    return StringType()  # Default fallback


# COMMAND ----------

