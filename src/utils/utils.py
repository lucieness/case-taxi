import yaml

# método que exclui todas as constraints de uma determinada tabela.
# não é utilizada a tabela information_schema.table_constraints pois ela não mostra todas as constraints
def remove_all_constraints(spark, table_name):
    df = spark.sql(f"DESCRIBE EXTENDED {table_name}")
    describe_data = df.collect()
    for row in describe_data:
        if row["data_type"].lower().startswith("foreign key"):
            spark.sql(f"ALTER TABLE {table_name} DROP CONSTRAINT {row['col_name']}")
    

# método responsável por incluir os metadados de determinada tabela. 
# ele lê um arquivo yaml com a configuração da tabela e colunas, tais como pk, fk, comentários, tags e atualiza os metadados
def create_update_table_metadata(spark, table_name, file_path):
    try:
        with open(file_path, "r") as file:
            metadata = yaml.safe_load(file)
            table_name_normalized = table_name.replace(".", "_")

            # para evitar conflito é necessário excluir todas as constraints da tabela
            remove_all_constraints(spark, table_name)

            table_comment = metadata.get("comment", "")
            if table_comment:
                spark.sql(f"COMMENT ON TABLE {table_name} IS '{table_comment}'")

            column_metadata = metadata.get("columns", {})
            for column, attributes in column_metadata.items():
                comment = attributes.get("comment", "")
                if comment:
                    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{comment}'")

                tags = attributes.get("tags", {})
                if tags:
                    tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
                    spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {column} SET TAGS ( {tag_str} )")

            primary_key = metadata.get("primary_key")
            if primary_key:
                # para ser chave primário o campo nâo pode ser do tipo nullable. 
                spark.sql(f"ALTER TABLE {table_name} ALTER COLUMN {primary_key} SET NOT NULL")
                spark.sql(f"ALTER TABLE {table_name} ADD CONSTRAINT {table_name_normalized}_pk PRIMARY KEY ({primary_key})")

            foreign_keys = metadata.get("foreign_keys", [])
            for fk in foreign_keys:
                spark.sql(
                    f"ALTER TABLE {table_name} ADD CONSTRAINT {table_name_normalized}_{fk['name']} "
                    f"FOREIGN KEY ({fk['column']}) REFERENCES {fk['reference_table']}({fk['reference_column']})"
                )

            table_tags = metadata.get("tags", {})
            if table_tags:
                tag_str = ", ".join([f"'{k}' = '{v}'" for k, v in table_tags.items()])
                spark.sql(f"ALTER TABLE {table_name} SET TAGS ( {tag_str} )")

    except Exception as e:
        print(f"Error updating table metadata: {e}")
