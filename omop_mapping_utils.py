import pandas as pd

# Method 1. compare direclty:
def compare_dataframes(df1, df2, merge_columns, usagi=True):
    """ Compare two DataFrames and merge them based on specified columns. 
    
    This function merges two DataFrames on the specified columns and compares the values in the remaining columns. 
    If the values match, it keeps the value; if they differ, it shows the difference in the format "data from left != data from right". 
    It also indicates if a row is missing in either the left or right DataFrame. 
    For usagi files, merge_columns shoud be ["sourceCode"].
    This function is useful for are one-to-one mappings, for one-to-many mappings also see compare_dfs.
    
    Parameters: 
    df1 (pd.DataFrame): The first DataFrame to compare. 
    df2 (pd.DataFrame): The second DataFrame to compare. 
    merge_columns (list): The list of columns to merge on. 
    
    Returns: pd.DataFrame: A DataFrame with the merged and compared results. 
    """

    # For usagi files, only keep critical columns for the comparision: 
    if usagi:
        print("USAGI mode")
        colstokeep = ["sourceCode","sourceName", "conceptId", "conceptName"]
        df1 = df1[colstokeep]
        df2 = df2[colstokeep]
    
    # Merge the DataFrames on the specified columns
    merged_df = pd.merge(df1, df2, on=merge_columns, how='outer', suffixes=('_left', '_right'), indicator=True)
    
    # Iterate through the columns to compare values
    #for col in df1.columns:
    #    if col not in merge_columns:
    #        merged_df[col] = merged_df.apply(
    #            lambda row: row[f'{col}_left'] if row[f'{col}_left'] == row[f'{col}_right'] else (
    #                f"{row[f'{col}_left']} != {row[f'{col}_right']}" if pd.notna(row[f'{col}_left']) and pd.notna(row[f'{col}_right']) else (
    #                    f"Missing in left: {row[f'{col}_right']}" if pd.isna(row[f'{col}_left']) else f"Missing in right: {row[f'{col}_left']}"
    #                )
    #            ),
    #            axis=1
    #        )
    #print(merged_df)
    
    for col in df1.columns: 
        if col not in merge_columns: 
            def compare_values(row): 
                left_value = row[f'{col}_left'] 
                right_value = row[f'{col}_right'] 
                
                if left_value == right_value: 
                    return left_value 
                elif pd.notna(left_value) and pd.notna(right_value): 
                    return f"{left_value} != {right_value}" 
                elif pd.isna(left_value): 
                    return f"Missing in left: {right_value}" 
                else: 
                    return f"Missing in right: {left_value}" 
                    
            merged_df[col] = merged_df.apply(compare_values, axis=1)
            
    #print(merged_df)
    
    # Indicate missing rows in the key columns
    #for col in merge_columns:
    #    merged_df[col] = merged_df.apply(
    #        lambda row: f"Missing in left: {row[f'{col}_right']}" if pd.isna(row[f'{col}_left']) else (
    #            f"Missing in right: {row[f'{col}_left']}" if pd.isna(row[f'{col}_right']) else row[col]
    #        ),
    #        axis=1
    #    )
    
    # Drop the suffix columns and the merge indicator
    #merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_left') or col.endswith('_right') or col == '_merge'], inplace=True)
    # ...but dont drop merge and 
    merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_left') or col.endswith('_right')], inplace=True)
    
    return merged_df


# Method 2: compare sets, good for one-to-many mappings
def compare_dfs(df1, df2, source_code_col='sourceCode', concept_id_col='conceptId', how='simple'):
    """
    Compare two mappings (DataFrames), e.g usagi, set based;
    by collapsing rows based on a sourceCode column and comparing the conceptId sets.  
    This function is usefull if there are one-to-many mappings, because the whole conceptId set is compared for each sourceCode. 

    Parameters:
    df1 (pd.DataFrame): The first input DataFrame to be compared.
    df2 (pd.DataFrame): The second input DataFrame to be compared.
    source_code_col (str): The name of the column to group by (default is 'sourceCode').
    concept_id_col (str): The name of the column containing the concept IDs to be compared (default is 'conceptId').
    how (str): Level of detail of comparision; simple, conceptName, full, (default is 'simple').

    Returns:
    pd.DataFrame: A DataFrame with the comparison results, indicating whether the conceptId sets match or differ.
    """
    df1_collapsed = collapse_rows(df1.copy(), how=how)
    df2_collapsed = collapse_rows(df2.copy(), how=how)
    result = compare_collapsed_dfs(df1_collapsed, df2_collapsed, source_code_col, concept_id_col)
    return result

# # Example usage
# df1 = pd.DataFrame({
#     'sourceCode': ['A01', 'A02', 'A03'],
#     'conceptId': [[1001, 1002], [1003], [1004, 1005]]
# })

# df2 = pd.DataFrame({
#     'sourceCode': ['A01', 'A02', 'A03'],
#     'conceptId': [[1001, 1002], [1003, 1006], [1004, 1005]]
# })

def compare_collapsed_dfs(df1, df2, source_code_col, concept_id_col):
    """
    Compare two collapsed mappings DataFrames by merging them on a sourceCode column and comparing the conceptId sets.

    Parameters:
    df1 (pd.DataFrame): The first collapsed DataFrame to be compared.
    df2 (pd.DataFrame): The second collapsed DataFrame to be compared.
    source_code_col (str): The name of the column to merge on.
    concept_id_col (str): The name of the column containing the concept IDs to be compared.

    Returns:
    pd.DataFrame: A DataFrame with the comparison results, indicating whether the concept ID sets match or differ.
    """
    # Merge the DataFrames on the sourceCode column
    merged_df = pd.merge(df1, df2, on=source_code_col, how='outer', suffixes=('_left', '_right'), indicator=True)
    
    # 1. Compare the conceptId sets
    def compare_concept_ids(row):
        left_value = row[f'{concept_id_col}_left']
        right_value = row[f'{concept_id_col}_right']
        
        left_set = set(left_value) if isinstance(left_value, list) else set()
        right_set = set(right_value) if isinstance(right_value, list) else set()
        
        if left_set == right_set:
            return 'Match'
        else:
            return 'Different'#f"Different: {left_set} != {right_set}"    
    
    # Add column that compares conceptId's        
    merged_df['Comparison'] = merged_df.apply(compare_concept_ids, axis=1)

    # 2. Compare all other columns eacheach columns and replace the original _left and _right values
    def compare_columns(row):
        for col in df1.columns:
            if col != source_code_col:
                left_value = row[f'{col}_left']
                right_value = row[f'{col}_right']
                
                left_set = set(left_value) if isinstance(left_value, list) else set()
                right_set = set(right_value) if isinstance(right_value, list) else set()
                
                if left_set == right_set:
                    row[col] = left_value
                else:
                    row[col] = f"{left_value} != {right_value}"
                #elif pd.notna(left_value) and pd.notna(right_value):
                #    row[col] = f"{left_value} != {right_value}"
                #elif pd.isna(left_value):
                #    row[col] = f"Missing in left: {right_value}"
                #else:
                #    row[col] = f"Missing in right: {left_value}"
        return row
    
    merged_df = merged_df.apply(compare_columns, axis=1)
    
    # Drop the original _left and _right columns
    merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_left') or col.endswith('_right')], inplace=True)
    
    return merged_df


# Utility functions:

def split_single_and_multiple_occurrences(df, column_name):
    # Count occurrences of each sourceCode
    occurrence_counts = df[column_name].value_counts()
    
    # Identify single and multiple occurrences
    single_occurrences = occurrence_counts[occurrence_counts == 1].index
    multiple_occurrences = occurrence_counts[occurrence_counts > 1].index
    
    # Split the DataFrame into two
    df_single = df[df[column_name].isin(single_occurrences)]
    df_multiple = df[df[column_name].isin(multiple_occurrences)]
    
    return df_single, df_multiple


def collapse_rows(df, source_code_col='sourceCode', concept_id_col='conceptId', how='simple'):
    """
    Collapse rows in a DataFrame by grouping on a specified source code column and aggregating the concept ID values into a list.

    Parameters:
    df (pd.DataFrame): The input DataFrame containing the data to be collapsed.
    source_code_col (str): The name of the column to group by (default is 'sourceCode').
    concept_id_col (str): The name of the column containing the concept IDs to be aggregated (default is 'conceptId').

    Returns:
    pd.DataFrame: A new DataFrame with rows collapsed by the source code column and concept IDs aggregated into lists.
    """
    # Group by the sourceCode column and aggregate the conceptId values into a list 
    if how=='simple':
        collapsed_df = df.groupby(source_code_col).agg({concept_id_col: list}).reset_index()
    elif how=='conceptName':
        collapsed_df = df.groupby(source_code_col).agg({'sourceName': list, concept_id_col: list, 'conceptName': list}).reset_index()
    elif how=='full':
        collapsed_df = df.groupby(source_code_col).agg({col: list for col in df.columns if col != source_code_col}).reset_index()

    return collapsed_df


def remove_common_rows(df1, df2, column_name):
    """
    Remove rows from the first DataFrame where the values in the specified column are present in the second DataFrame.

    Parameters:
    df1 (pd.DataFrame): The first input DataFrame from which rows will be removed.
    df2 (pd.DataFrame): The second input DataFrame containing the values to be removed from df1.
    column_name (str): The name of the column to check for common values.

    Returns:
    pd.DataFrame: A new DataFrame with rows removed from df1 where the specified column's values are present in df2.
    """
    # Get the unique sourceCodes from df2 
    common_source_codes = df2[column_name].unique()
    # Filter out rows in df1 where sourceCode is in df2 
    df1_filtered = df1[~df1[column_name].isin(common_source_codes)]
    return df1_filtered

# Define a function to limit characters in each column
def limit_characters(value, max_chars=20):
    if len(str(value))>max_chars:
        value = str(value)[:max_chars] + '...'
    return value
