# -*- coding: utf-8 -*-
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import ArrayType, StructType


class DataFrameUtils:
    @staticmethod
    def flatten_data_frame(nested_df: DataFrame) -> DataFrame:
        """
        flatten_data_frame Flatten the nested dataframe

        Takes nested dataframe with StructType and or ArrayType fields and
        returns the flattened dataframe after flattening all the nested columns

        Parameters
        ----------
        nested_df : DataFrame
            Dataframe to be flatten

        Returns
        -------
        DataFrame
            Return the single level flattened dataframe
        """
        from pyspark.sql.functions import col

        df_schema = nested_df.schema
        fields = df_schema.fields
        field_names = df_schema.fieldNames()

        for i in range(len(field_names)):
            field = fields[i]
            field_name = field.name
            field_data_type = field.dataType

            if isinstance(field_data_type, ArrayType):
                field_name_excluding_array = list(
                    filter(lambda col_name: col_name != field_name, field_names)
                )
                field_names_and_explode = field_name_excluding_array + [
                    f"posexplode_outer({field_name}) as ({field_name}_pos, {field_name})"
                ]
                array_df = nested_df.selectExpr(*field_names_and_explode)
                return DataFrameUtils.flatten_data_frame(array_df)

            elif isinstance(field_data_type, StructType):
                child_fieldnames = field_data_type.names
                struct_field_names = [f"{field_name}.{childname}" for childname in child_fieldnames]
                new_field_names = (
                    list(filter(lambda col_name: col_name != field_name, field_names)) + struct_field_names
                )
                renamed_cols = map(lambda x: x.replace(".", "_"), new_field_names)  # noqa: C417
                zip_alias_col_names = zip(new_field_names, renamed_cols)  # noqa: B905
                alias_col_names = map(lambda y: col(y[0]).alias(y[1]), zip_alias_col_names)  # noqa
                struct_df = nested_df.select(*alias_col_names)
                return DataFrameUtils.flatten_data_frame(struct_df)

        return nested_df
